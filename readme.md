## Kafka Connect S3 csv formatter
This is a formatter which takes AVRO records and generates CSV files in S3.

### How to use it?
Run maven package to generate the jar. Then copy it into the S3 sink connector folder inside the plugins directory.


### Things to note.
* Works with `io.confluent.connect.avro.AvroConverter`
* If schema changes in between a flush, the csv will be commited with old schema and new schema based csv file will start getting generated.
* To make this process work, I am appending the Schema Registry version number.
* this is an example configuration 
```
{
  "name":"testenv",
  "config":
    {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "topics": "test-envvariables",
    "format.class":"io.confluent.connect.s3.format.bytearray.ByteArrayFormat",
    "value.converter":"org.apache.kafka.connect.converters.ByteArrayConverter",
    "flush.size":"5",
    "s3.bucket.name":"BUCKET_NAME"
  }
}
```
