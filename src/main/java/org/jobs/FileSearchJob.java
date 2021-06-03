package org.jobs;

import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;

import static org.jobs.FileUtils.setupAndGetOutputFormat;

public class FileSearchJob {

	public static void main(String[] args) throws Exception {
		String filename = args[0];
		FileUtils.fetchAndExtractS3File(filename);

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		String tempFilePath = "./tmp";
		DataSet<String> stringStream = env.readTextFile(tempFilePath);

		DataSet<Tuple2<Void, IndexedRecord>> recordStream = FileSearchFunctions.processStream(stringStream);

		HadoopOutputFormat<Void, IndexedRecord> hadoopOutputFormat = setupAndGetOutputFormat(filename);
		recordStream.output(hadoopOutputFormat);
		// execute program
		env.execute("Flink Batch Java API Skeleton");
	}
}
