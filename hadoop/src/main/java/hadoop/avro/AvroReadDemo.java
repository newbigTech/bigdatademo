package hadoop.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;

public class AvroReadDemo {

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
    public static final String SCHEMA_JSON = "{\"namespace\": \"com.hadoop.avro\",\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"HdfsUser\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"user_id\", \"type\": \"long\"}\n" +
            "  ]\n" +
            "}";
    public static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);


    public static Path getHdfsPath(String path) {
        return new Path(String.format(new StringBuilder().append(path).append("_%s").toString(), new DateTime().toString("yyyyMMdd000000")));
    }

    public static void main(String[] args) throws Exception {
        String base = "file/demo";
        Path basePath = getHdfsPath(base);
//        File file = new File(basePath.toString()+"/000000_0");
        File file = new File("/Users/haibo/Desktop/000000_0.0");
        DatumReader<GenericRecord> reader = new GenericDatumReader<>();

        try (DataFileReader<GenericRecord> dataFileReader =
                     new DataFileReader<GenericRecord>(file, reader)) {
            GenericRecord record;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next();

                System.out.println(record.get("user_id"));
            }
        }
    }
}