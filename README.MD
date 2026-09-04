# Resilient Flink ZIP CSV job

This Flink DataStream job reads one or more ZIP files, processes only safe `.csv` entries in parallel, validates each CSV row against `schema.avsc`, and writes accepted records as checkpoint-committed Parquet. Malformed rows are written to a separate checkpoint-committed quarantine path.

## Safety and reliability

- ZIP archives are streamed directly from the configured filesystem; no shared local extraction directory is used.
- Archive size, uncompressed entry size, and entry count are bounded. Absolute paths, traversal-like names, directories, and non-CSV entries are ignored.
- Completed archive identifiers are checkpointed. The job uses exactly-once checkpoints, retained externalized checkpoints, RocksDB state, fixed-delay restart, and checkpoint-committed `FileSink` outputs.
- Flink metrics include `acceptedRecords`, `rejectedRecords`, and `failedArchives`.

## Build

Use Java 17 and Maven:

```sh
mvn clean verify
```

For a cluster artifact, run `mvn package`. Flink runtime libraries and the RocksDB backend are provided by the cluster. Install Flink's matching S3 filesystem plugin when using `s3://` paths; do not package cloud credentials into the artifact.

## Run

```sh
flink run -c org.jobs.FileSearchJob target/file-jobs-1.0-SNAPSHOT.jar \
  --input s3://input-bucket/a.zip,s3://input-bucket/b.zip \
  --output s3://output-bucket/records \
  --quarantine s3://output-bucket/quarantine \
  --checkpoint-dir s3://state-bucket/checkpoints \
  --aws-region us-east-1 \
  --filter ellipsis \
  --parallelism 4
```

`--input`, `--output`, and `--checkpoint-dir` are required. Optional limits are `--checkpoint-seconds` (default `60`), `--max-entries` (default `10000`), `--max-archive-bytes` (default `1073741824`), and `--max-entry-bytes` (default `134217728`). All must be positive.

Each input CSV row must contain the same number of fields as `schema.avsc`; the included schema has one `line` field. Quarantine rows include archive URI, ZIP entry, row number, reason, and original row.
