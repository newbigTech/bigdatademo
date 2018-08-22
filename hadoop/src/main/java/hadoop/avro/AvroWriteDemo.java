package com.enniu.cloud.services.avro;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.joda.time.DateTime;

public class AvroWriteDemo {

    private static FileSystem fs;
    static {
//        System.setProperty("HADOOP_USER_NAME", "libin1");
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS","hdfs://slave132:8020");
//        conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static final String SCHEMA_JSON = "{\"namespace\": \"com.enniu.hadoop.avro\",\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"HdfsUser\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"user_id\", \"type\": \"long\"}\n" +
            "  ]\n" +
            "}";
    public static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

    /**
     * get whitelist path
     *
     * @param path eg: /user/hive/warehouse/dm_risk_prod.db/dim_rpd_t_whitelist_foroperation
     */
    public static Path getHdfsPath(String path) {
        return new Path(String.format(new StringBuilder().append(path).append("_%s").toString(), new DateTime().toString("yyyyMMdd000000")));
    }
    public static void writeToAvro(OutputStream outputStream)
            throws IOException {
        try (DataFileWriter<Object> writer = new DataFileWriter<>(
                new GenericDatumWriter<>()).setSyncInterval(100)) {
            writer.setCodec(CodecFactory.snappyCodec());
            writer.create(SCHEMA, outputStream);
            for (long userId = 1; userId < 100L; userId++) {
                GenericRecord record = new GenericData.Record(SCHEMA);
                record.put("user_id", userId);
                writer.append(record);
            }
            IOUtils.cleanup(null, writer);
        }
        IOUtils.cleanup(null, outputStream);
    }

    public static void main(String[] args) throws Exception {
        String base = "file/demo";
        Path basePath = getHdfsPath(base);
        if(fs.exists(basePath)){
            fs.delete(basePath,true);
        }
        fs.mkdirs(basePath);
        OutputStream os = fs.create(new Path(basePath,"000000_0"));
        writeToAvro( os);
        fs.create(new Path(basePath,"_SUCCESS"));
    }
}