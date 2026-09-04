package org.jobs;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

final class ZipCsvSource extends RichParallelSourceFunction<ZipCsvSource.ZipLine> implements CheckpointedFunction {
    private final List<String> archives;
    private final int maxEntries;
    private final long maxArchiveBytes;
    private final long maxEntryBytes;
    private transient ListState<String> completedState;
    private transient Set<String> completed;
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
            if (completed.contains(archive)) continue;
            try {
                readArchive(archive, context);
                synchronized (context.getCheckpointLock()) {
                    completed.add(archive);
                }
            } catch (IOException exception) {
                failedArchives.inc();
                throw new IOException("Unable to process ZIP archive " + archive, exception);
            }
        }
    }

    private void readArchive(String archive, SourceContext<ZipLine> context) throws IOException {
        Path path = new Path(archive);
        FileSystem fileSystem = path.getFileSystem();
        try (InputStream raw = fileSystem.open(path);
             InputStream limited = new LimitedInputStream(raw, maxArchiveBytes, "archive");
             ZipInputStream zip = new ZipInputStream(limited, StandardCharsets.UTF_8)) {
            int entries = 0;
            ZipEntry entry;
            while (running && (entry = zip.getNextEntry()) != null) {
                if (++entries > maxEntries) throw new IOException("ZIP entry limit exceeded");
                if (entry.isDirectory() || !isCsvEntry(entry.getName())) {
                    zip.closeEntry();
                    continue;
                }
                BufferedReader lines = new BufferedReader(new InputStreamReader(
                        new LimitedInputStream(zip, maxEntryBytes, "entry " + entry.getName()), StandardCharsets.UTF_8));
                String line;
                long lineNumber = 0;
                while (running && (line = lines.readLine()) != null) {
                    lineNumber++;
                    synchronized (context.getCheckpointLock()) {
                        context.collect(new ZipLine(archive, entry.getName(), lineNumber, line));
                    }
                }
                zip.closeEntry();
            }
        }
    }

    static boolean isCsvEntry(String name) {
        return name != null && !name.startsWith("/") && !name.contains("..") && name.toLowerCase().endsWith(".csv");
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        completedState.update(new java.util.ArrayList<>(completed));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        completedState = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>("completed-archives", StringSerializer.INSTANCE));
        completed = new HashSet<>();
        for (String archive : completedState.get()) completed.add(archive);
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
