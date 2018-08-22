package com.enniu.cloud.services.avro;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvroSort extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroSort(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance(getConf(),"AvroSort");

        job.setJarByClass(getClass());
        //使用用户avro版本
        job.getConfiguration().setBoolean(Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST,true);

        FileInputFormat.addInputPath(job,new Path("hdfs://hadoop:9000/user/madong/avro/pairs.avro"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://hadoop:9000/user/madong/avro-out"));

        AvroJob.setDataModelClass(job, GenericData.class);

        Schema schema = new Schema.Parser().parse(new File("/data/workspace/hadoop/src/main/resources/SortedStringPair.avsc"));

        AvroJob.setInputKeySchema(job,schema);
        // AvroKey<K>,AvroValue<K>
        AvroJob.setMapOutputKeySchema(job,schema);
        AvroJob.setMapOutputValueSchema(job,schema);
        //AvroKey<K>,NullWritable
        AvroJob.setOutputKeySchema(job,schema);

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(NullWritable.class);


        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class SortMapper<K> extends Mapper<AvroKey<K>,NullWritable,AvroKey<K>,AvroValue<K>>{

        @Override
        protected void map(AvroKey<K> key, NullWritable value, Context context) throws IOException, InterruptedException {
            context.write(key,new AvroValue<K>(key.datum()));
        }
    }

    static class SortReducer<K> extends Reducer<AvroKey<K>,AvroValue<K>,AvroKey<K>,NullWritable>{

        @Override
        protected void reduce(AvroKey<K> key, Iterable<AvroValue<K>> values, Context context) throws IOException, InterruptedException {

            for (AvroValue<K> value : values){
                context.write(new AvroKey<K>(value.datum()),NullWritable.get());
            }
        }
    }
}
