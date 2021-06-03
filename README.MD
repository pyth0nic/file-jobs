# File Job Example
A simple flink application which ingests a zip from S3, filters lines in the csv's within the zip,
outputs parquet to s3 with the relevant lines.

### Why Flink
todo

### Environment Variables
Ensure hadoop 3.3 is installed and HADOOP_HOME variable is set.
Ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set.

### Running

#### Debugging/Testing
To run open in intellij and navigate to ```FileSearchJob``` class, set cli variable in run configuration

Ensure bucket name is correct in FileUtils (todo add to config) 