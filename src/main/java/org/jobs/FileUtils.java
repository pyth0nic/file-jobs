package org.jobs;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.*;
import java.util.zip.ZipInputStream;

public class FileUtils {

    public final static String SCHEMA_PATH = "schema.avsc";
    private final static String BUCKET = "test-nnn";

    public static InputStream getS3File(String filename, String bucketName) {
        AmazonS3 s3Client = new AmazonS3Client(new EnvironmentVariableCredentialsProvider());
        S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, filename));
        return object.getObjectContent();
    }

    public static void readFile(InputStream is) throws IOException {
        ZipInputStream zis = new ZipInputStream(is);

        FileOutputStream fos = new FileOutputStream("./tmp");
        byte[] buf = new byte[32 * 1024];
        while (zis.getNextEntry() != null) {
            int numRead;
            while ((numRead = zis.read(buf)) >= 0) {
                fos.write(buf, 0, numRead);
            }
        }
    }

    public static HadoopOutputFormat<Void, IndexedRecord> setupAndGetOutputFormat(String filename) throws IOException {
        Job job = Job.getInstance();
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, CompressionCodecName.SNAPPY.getHadoopCompressionCodecClass());
        FileOutputFormat.setOutputPath(job, getOutputDir(filename));

        HadoopOutputFormat<Void, IndexedRecord>  hadoopOutputFormat = new HadoopOutputFormat<>(new AvroParquetOutputFormat<>(), job);
        final Schema schema = new Schema.Parser().parse(CSV.class.getClassLoader().getResourceAsStream(FileUtils.SCHEMA_PATH));
        AvroParquetOutputFormat.setSchema(job, schema);
        return hadoopOutputFormat;
    }

    private static Path getOutputDir(String filename) {
        return new Path(String.format("s3a://%s/%s", BUCKET, filename.replace(".zip", "")));
    }

    public static void fetchAndExtractLocalFile(String filename) throws IOException {
        File file = new File(filename);
        FileInputStream is = new FileInputStream(file);
        FileUtils.readFile(is);
    }

    public static void fetchAndExtractS3File(String filename) throws IOException {
        InputStream is = FileUtils.getS3File(filename, BUCKET);
        FileUtils.readFile(is);
    }
}
