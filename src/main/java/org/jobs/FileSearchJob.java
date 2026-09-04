package org.jobs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.rollingsink.OnCheckpointRollingPolicy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;

public class FileSearchJob {
    public static void main(String[] args) throws Exception {
        JobConfig job = JobConfig.parse(args);
        Configuration configuration = new Configuration();
        if (!job.awsRegion.isBlank()) configuration.setString("s3.endpoint.region", job.awsRegion);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(job.parallelism);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
        env.enableCheckpointing(job.checkpointInterval.toMillis(), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(job.checkpointDirectory);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));

        DataStream<ZipCsvSource.ZipLine> lines = env
                .addSource(new ZipCsvSource(job.inputs, job.maxEntries, job.maxArchiveBytes, job.maxEntryBytes))
                .name("safe-zip-csv-source")
                .setParallelism(job.parallelism);
        DataStream<GenericRecord> records = lines.process(new CsvRecordProcessor(job.filter))
                .name("validate-csv-records");

        Schema schema = new Schema.Parser().parse(FileSearchJob.class.getResourceAsStream("/schema.avsc"));
        FileSink<GenericRecord> recordsSink = FileSink
                .forBulkFormat(new Path(job.output), ParquetAvroWriters.forGenericRecord(schema))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();
        FileSink<String> quarantineSink = FileSink
                .forRowFormat(new Path(job.quarantine), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();
        records.sinkTo(recordsSink).name("atomic-parquet-sink");
        records.getSideOutput(CsvRecordProcessor.REJECTED_RECORDS)
                .sinkTo(quarantineSink).name("malformed-record-quarantine");
        env.execute("Resilient ZIP CSV to Parquet job");
    }
}
