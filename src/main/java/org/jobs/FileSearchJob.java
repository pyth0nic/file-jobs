package org.jobs;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.*;
import java.util.zip.ZipInputStream;

public class FileSearchJob {

	private static InputStream getS3File(String filename) {
		AmazonS3 s3Client = new AmazonS3Client(new EnvironmentVariableCredentialsProvider());
		S3Object object = s3Client.getObject(new GetObjectRequest("test-nnn", filename));
		InputStream objectData = object.getObjectContent();
		return objectData;
	}

	private static void readFile(InputStream is) throws IOException {
		ZipInputStream zis = new ZipInputStream(is);

		FileOutputStream fos = new FileOutputStream("./tmp");
		byte[] buf = new byte[32 * 1024];
		while(zis.getNextEntry() != null) {
			int numRead;
			while ( (numRead = zis.read(buf)) >= 0) {
				fos.write(buf, 0 , numRead);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		//File file = new File("./x.zip");
		//FileInputStream is = new FileInputStream(file);
		InputStream is = getS3File("x.zip");

		readFile(is);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple2<Void, IndexedRecord>> csvStream = env.readTextFile("./tmp")
		.filter(x-> x.contains("ellipsis"))
		.map(new MapFunction<String, Tuple2<Void, IndexedRecord>>() {
			@Override
			public Tuple2<Void, IndexedRecord> map(String x) throws Exception {
				return Tuple2.of(null, new CSV(x));
			}
		});

		Job job = Job.getInstance();
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, CompressionCodecName.SNAPPY.getHadoopCompressionCodecClass());
		HadoopOutputFormat<Void, IndexedRecord>  hadoopOutputFormat = new HadoopOutputFormat<>(new AvroParquetOutputFormat<IndexedRecord>(), job);
		FileOutputFormat.setOutputPath(job, new Path("s3a://test-nnn/my-parquet"));
		final Schema schema = new Schema.Parser().parse(CSV.class.getClassLoader().getResourceAsStream("schema.avsc"));
		AvroParquetOutputFormat.setSchema(job, schema);

		csvStream.output(hadoopOutputFormat);
		// execute program
		env.execute("Flink Batch Java API Skeleton");
	}

	public static class CSV implements IndexedRecord {
		private final String line;

		private static  Schema schema;

		public CSV(String line) {
			this.line = line;
		}

		static {
			try {
				schema = new Schema.Parser().parse(CSV.class.getClassLoader().getResourceAsStream("schema.avsc"));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void put(int i, Object o) {
			throw new NotImplementedException("You can't update this IndexedRecord");
		}

		@Override
		public Object get(int i) {
			return line;
		}

		@Override
		public Schema getSchema() {
			return schema;
		}
	}
}
