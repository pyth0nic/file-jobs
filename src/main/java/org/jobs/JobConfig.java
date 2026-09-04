package org.jobs;

import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class JobConfig implements Serializable {
    final List<String> inputs;
    final String output;
    final String quarantine;
    final String filter;
    final String checkpointDirectory;
    final String awsRegion;
    final int parallelism;
    final Duration checkpointInterval;
    final int maxEntries;
    final long maxArchiveBytes;
    final long maxEntryBytes;

    private JobConfig(List<String> inputs, String output, String quarantine, String filter,
                      String checkpointDirectory, String awsRegion, int parallelism,
                      Duration checkpointInterval, int maxEntries, long maxArchiveBytes, long maxEntryBytes) {
        this.inputs = inputs;
        this.output = output;
        this.quarantine = quarantine;
        this.filter = filter;
        this.checkpointDirectory = checkpointDirectory;
        this.awsRegion = awsRegion;
        this.parallelism = parallelism;
        this.checkpointInterval = checkpointInterval;
        this.maxEntries = maxEntries;
        this.maxArchiveBytes = maxArchiveBytes;
        this.maxEntryBytes = maxEntryBytes;
    }

    static JobConfig parse(String[] args) {
        Map<String, String> values = new java.util.HashMap<>();
        for (int index = 0; index < args.length; index += 2) {
            if (!args[index].startsWith("--") || index + 1 == args.length || values.put(args[index], args[index + 1]) != null) {
                throw new IllegalArgumentException(usage());
            }
            Set<String> supported = Set.of("--input", "--output", "--quarantine", "--filter", "--checkpoint-dir",
                    "--aws-region", "--parallelism", "--checkpoint-seconds", "--max-entries", "--max-archive-bytes",
                    "--max-entry-bytes");
            if (!supported.containsAll(values.keySet())) {
                throw new IllegalArgumentException(usage());
            }
        }
        String input = required(values, "--input");
        String output = required(values, "--output");
        String checkpointDirectory = required(values, "--checkpoint-dir");
        String quarantine = values.getOrDefault("--quarantine", output + "-quarantine");
        int parallelism = positiveInt(values.getOrDefault("--parallelism", "1"), "--parallelism");
        long checkpointSeconds = positive(values.getOrDefault("--checkpoint-seconds", "60"), "--checkpoint-seconds");
        int maxEntries = positiveInt(values.getOrDefault("--max-entries", "10000"), "--max-entries");
        long maxArchiveBytes = positive(values.getOrDefault("--max-archive-bytes", "1073741824"), "--max-archive-bytes");
        long maxEntryBytes = positive(values.getOrDefault("--max-entry-bytes", "134217728"), "--max-entry-bytes");
        List<String> inputs = Arrays.stream(input.split(",")).map(String::trim).filter(value -> !value.isEmpty()).toList();
        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("--input must contain at least one ZIP URI");
        }
        if (output.equals(quarantine) || output.equals(checkpointDirectory) || quarantine.equals(checkpointDirectory)) {
            throw new IllegalArgumentException("output, quarantine, and checkpoint paths must be distinct");
        }
        return new JobConfig(inputs, output, quarantine, values.getOrDefault("--filter", ""),
                checkpointDirectory, values.getOrDefault("--aws-region", ""), parallelism,
                Duration.ofSeconds(checkpointSeconds), maxEntries, maxArchiveBytes, maxEntryBytes);
    }

    private static String required(Map<String, String> values, String key) {
        String value = values.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(usage());
        }
        return value;
    }

    private static int positiveInt(String value, String key) {
        try {
            int result = Integer.parseInt(value);
            if (result > 0) return result;
        } catch (NumberFormatException ignored) { }
        throw new IllegalArgumentException(key + " must be a positive integer");
    }

    private static long positive(String value, String key) {
        try {
            long result = Long.parseLong(value);
            if (result > 0) return result;
        } catch (NumberFormatException ignored) { }
        throw new IllegalArgumentException(key + " must be a positive integer");
    }

    static String usage() {
        return "Usage: --input <zip-uri[,zip-uri]> --output <uri> --checkpoint-dir <uri> "
                + "[--quarantine <uri>] [--filter <text>] [--aws-region <region>] [--parallelism <n>] "
                + "[--checkpoint-seconds <n>] [--max-entries <n>] [--max-archive-bytes <n>] [--max-entry-bytes <n>]";
    }
}
