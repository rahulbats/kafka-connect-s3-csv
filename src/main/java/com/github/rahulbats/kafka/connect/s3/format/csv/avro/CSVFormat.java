package com.github.rahulbats.kafka.connect.s3.format.csv.avro;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;

public class CSVFormat implements Format<S3SinkConnectorConfig, String> {
    private final S3Storage storage;
    private final AvroData avroData;

    public CSVFormat(S3Storage storage) {
        this.storage=storage;
        this.avroData = new AvroData(storage.conf().avroDataConfig());
    }

    @Override
    public RecordWriterProvider<S3SinkConnectorConfig> getRecordWriterProvider() {
        return new CSVRecordWriterProvider(storage,avroData);
    }

    @Override
    public SchemaFileReader<S3SinkConnectorConfig, String> getSchemaFileReader() {
        throw new UnsupportedOperationException("Reading schemas from S3 is not currently supported");
    }

    @Override
    public Object getHiveFactory() {
        throw new UnsupportedOperationException(
                "Hive integration is not currently supported in S3 Connector"
        );
    }
}
