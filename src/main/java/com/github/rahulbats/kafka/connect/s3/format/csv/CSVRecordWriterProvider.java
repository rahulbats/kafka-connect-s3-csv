package com.github.rahulbats.kafka.connect.s3.format.csv;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.format.RecordViewSetter;
import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.format.bytearray.ByteArrayFormat;
import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import java.io.IOException;
import java.io.OutputStream;

import static io.confluent.connect.s3.util.Utils.getAdjustedFilename;

public class CSVRecordWriterProvider extends RecordViewSetter implements RecordWriterProvider<S3SinkConnectorConfig> {

    private static final Logger log = LoggerFactory.getLogger(CSVRecordWriterProvider.class);
    private static final String EXTENSION = ".csv";
    private final S3Storage storage;
    private final AvroData avroData;

    CSVRecordWriterProvider(S3Storage storage, AvroData avroData){
        this.storage = storage;
        this.avroData = avroData;
        log.info("this is avrodata conf "+storage.conf().values());
    }

    @Override
    public String getExtension() {
        return EXTENSION;
    }

    @Override
    public RecordWriter getRecordWriter(S3SinkConnectorConfig s3SinkConnectorConfig, String filename) {
        log.info("Creating record writer");
        return new RecordWriter() {
            String adjustedFilename;
            //final String adjustedFilename = getAdjustedFilename(recordView, filename, getExtension());


            Schema schema = null;
            S3OutputStream s3out;
            OutputStream s3outWrapper;
            String outputString;
            //long recordCount=0;

            private void initWriters(SinkRecord record){
                //recordCount=0;
                schema = recordView.getViewSchema(record, false);
                adjustedFilename = filename +'-'+ schema.version() + recordView.getExtension() + EXTENSION;
                log.info("Opening record writer for: {}", adjustedFilename);
                s3out = storage.create(adjustedFilename, true, ByteArrayFormat.class);
                s3outWrapper = s3out.wrapForCompression();
                org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);

                String columnNames = schema.fields().stream().map(field -> field.name()).reduce((oldValue, newValue)->oldValue+","+newValue).get();
                columnNames=columnNames+"\n";
                try {
                    s3outWrapper.write(columnNames.getBytes());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


            @Override
            public void write(SinkRecord record) {
                if (schema == null) {
                    initWriters(record);
                } else if(schema.version()!=recordView.getViewSchema(record, false).version()) {
                    this.commit();
                    initWriters(record);
                }
                log.trace("Sink record with view {}: {}", recordView, record);
                GenericData.Record value = (GenericData.Record) avroData.fromConnectData(schema, recordView.getView(record, false));
                try {
                    StringBuffer sb = new StringBuffer();
                    outputString = schema.fields().stream().map(field ->String.valueOf( value.get(field.name()))).reduce((oldValue, newValue)->oldValue+","+newValue).get();
                    s3outWrapper.write(outputString.getBytes());
                    s3outWrapper.write("\n".getBytes());
                    //recordCount++;
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            }

            @Override
            public void close() {
                try {
                    //s3outWrapper.write(String.valueOf(recordCount).getBytes());
                    s3out.commit();
                    s3outWrapper.close();
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            }

            @Override
            public void commit() {
                try {
                    //s3outWrapper.write(String.valueOf(recordCount).getBytes());
                    // Flush is required here, because closing the writer will close the underlying S3
                    // output stream before committing any data to S3.
                    s3out.commit();
                    s3outWrapper.close();
                } catch (IOException e) {
                    throw new RetriableException(e);
                }
            }
        };
    }
}
