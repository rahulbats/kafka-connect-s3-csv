/*
Copyright 2021 Rahul Bhattacharya

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package com.github.rahulbats.kafka.connect.s3.format.csv.avro;

import com.github.rahulbats.kafka.connect.s3.format.csv.avro.CSVRecordWriterProvider;
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
