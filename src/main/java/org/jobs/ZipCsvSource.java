package org.jobs;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.BufferedReader;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

final class ZipCsvSource extends RichParallelSourceFunction<ZipCsvSource.ZipLine> implements CheckpointedFunction {
    private final List<String> archives;
    private final int maxEntries;
    private final long maxArchiveBytes;
    private final long maxEntryBytes;
    private transient ListState<ArchiveProgress> progressState;
    private transient Map<String, ArchiveProgress> progress;
    private transient Counter failedArchives;
    private volatile boolean running = true;

    ZipCsvSource(List<String> archives, int maxEntries, long maxArchiveBytes, long maxEntryBytes) {
        this.archives = archives;
        this.maxEntries = maxEntries;
        this.maxArchiveBytes = maxArchiveBytes;
        this.maxEntryBytes = maxEntryBytes;
    }

    @Override
    public void open(Configuration parameters) {
        failedArchives = getRuntimeContext().getMetricGroup().counter("failedArchives");
    }

    @Override
    public void run(SourceContext<ZipLine> context) throws Exception {
        int subtask = getRuntimeContext().getIndexOfThisSubtask();
        int subtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        for (int index = subtask; running && index < archives.size(); index += subtasks) {
            String archive = archives.get(index);
            ArchiveProgress checkpoint = progress.get(archive);
            if (checkpoint != null && checkpoint.completed) continue;
            try {
                readArchive(archive, checkpoint, context);
                synchronized (context.getCheckpointLock()) {
                    synchronized (progress) {
                        progress.put(archive, ArchiveProgress.completed(archive));
                    }
                }
            } catch (IOException exception) {
                failedArchives.inc();
                throw new IOException("Unable to process ZIP archive " + archive, exception);
            }
        }
    }

    private void readArchive(String archive, ArchiveProgress checkpoint, SourceContext<ZipLine> context) throws IOException {
        Path path = new Path(archive);
        FileSystem fileSystem = path.getFileSystem();
        try (InputStream raw = fileSystem.open(path);
             InputStream limited = new LimitedInputStream(raw, maxArchiveBytes, "archive");
             ZipInputStream zip = new ZipInputStream(limited, StandardCharsets.UTF_8)) {
            int entries = 0;
            boolean foundCheckpointEntry = checkpoint == null;
            ZipEntry entry;
            while (running && (entry = zip.getNextEntry()) != null) {
                if (++entries > maxEntries) throw new IOException("ZIP entry limit exceeded");
                if (entry.isDirectory() || !isCsvEntry(entry.getName())) {
                    zip.closeEntry();
                    continue;
                }
                if (!foundCheckpointEntry) {
                    if (!entry.getName().equals(checkpoint.entry)) {
                        zip.closeEntry();
                        continue;
                    }
                    foundCheckpointEntry = true;
                }
                BufferedReader lines = new BufferedReader(new InputStreamReader(
                        new LimitedInputStream(zip, maxEntryBytes, "entry " + entry.getName()), StandardCharsets.UTF_8));
                String line;
                long lineNumber = 0;
                while (running && (line = lines.readLine()) != null) {
                    lineNumber++;
                    if (checkpoint != null && entry.getName().equals(checkpoint.entry)
                            && lineNumber <= checkpoint.lineNumber) {
                        continue;
                    }
                    synchronized (context.getCheckpointLock()) {
                        context.collect(new ZipLine(archive, entry.getName(), lineNumber, line));
                        synchronized (progress) {
                            progress.put(archive, ArchiveProgress.at(archive, entry.getName(), lineNumber));
                        }
                    }
                }
                zip.closeEntry();
            }
            if (running && !foundCheckpointEntry) {
                throw new IOException("Checkpoint entry no longer exists in archive");
            }
        }
    }

    static boolean isCsvEntry(String name) {
        if (name == null) return false;
        String normalized = name.replace('\\', '/');
        if (normalized.startsWith("/") || normalized.startsWith("//") || normalized.matches("^[A-Za-z]:/.*")) {
            return false;
        }
        for (String segment : normalized.split("/")) {
            if (segment.equals("..")) return false;
        }
        return normalized.toLowerCase(Locale.ROOT).endsWith(".csv");
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        synchronized (progress) {
            progressState.update(new java.util.ArrayList<>(progress.values()));
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        progressState = context.getOperatorStateStore().getUnionListState(
                new ListStateDescriptor<>("archive-progress", ArchiveProgress.class));
        progress = new HashMap<>();
        for (ArchiveProgress checkpoint : progressState.get()) {
            ArchiveProgress existing = progress.get(checkpoint.archive);
            if (existing == null || checkpoint.completed
                    || (!existing.completed && checkpoint.lineNumber > existing.lineNumber)) {
                progress.put(checkpoint.archive, checkpoint);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    static final class ZipLine implements java.io.Serializable {
        final String archive;
        final String entry;
        final long lineNumber;
        final String line;

        ZipLine(String archive, String entry, long lineNumber, String line) {
            this.archive = archive;
            this.entry = entry;
            this.lineNumber = lineNumber;
            this.line = line;
        }

    }

    public static final class ArchiveProgress implements java.io.Serializable {
        public String archive;
        public String entry;
        public long lineNumber;
        public boolean completed;

        public ArchiveProgress() { }

        private ArchiveProgress(String archive, String entry, long lineNumber, boolean completed) {
            this.archive = archive;
            this.entry = entry;
            this.lineNumber = lineNumber;
            this.completed = completed;
        }

        static ArchiveProgress at(String archive, String entry, long lineNumber) {
            return new ArchiveProgress(archive, entry, lineNumber, false);
        }

        static ArchiveProgress completed(String archive) {
            return new ArchiveProgress(archive, null, 0, true);
        }
    }

    private static final class LimitedInputStream extends FilterInputStream {
        private final long limit;
        private final String description;
        private long read;

        private LimitedInputStream(InputStream input, long limit, String description) {
            super(input);
            this.limit = limit;
            this.description = description;
        }

        @Override
        public int read() throws IOException {
            int value = super.read();
            if (value >= 0) increment(1);
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            int count = super.read(bytes, offset, length);
            if (count > 0) increment(count);
            return count;
        }

        private void increment(long count) throws IOException {
            read += count;
            if (read > limit) throw new IOException(description + " size limit exceeded");
        }
    }
}
