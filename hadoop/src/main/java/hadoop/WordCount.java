package org.apache.hadoop;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class WordCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(DiffReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileSystem fs = FileSystem.get(conf);
        Path outputpath = new Path("output");
        if (fs.exists(outputpath)) {
            fs.delete(outputpath, true);
        }
        FileInputFormat.addInputPath(job, new Path("input/aa*"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        // 注册添加NameOutput--counters计算结果存放文件路径，注意此处要使用
        // org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class
        MultipleOutputs.addNamedOutput(job, "counter", TextOutputFormat.class, Text.class, LongWritable.class);
        Path outputPath = org.apache.hadoop.mapred.FileOutputFormat.getOutputPath((JobConf) job.getConfiguration());
        System.out.println(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String v = value.toString();
            String[] c = v.split(" ");
            word.set(c[0]);
            context.write(word, new IntWritable(Integer.valueOf(c[1])));
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private MultipleOutputs multipleOutputs;
        private Map<String, LongAdder> counter;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 初始化MultipleOutputs对象
            multipleOutputs = new MultipleOutputs(context);
            counter = Maps.newConcurrentMap();
            super.setup(context);
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
                if (counter.containsKey(key.toString())) {
                    counter.get(key.toString()).increment();
                } else {
                    LongAdder longAdder = new LongAdder();
                    longAdder.increment();
                    counter.put(key.toString(), longAdder);
                }
                multipleOutputs.write("counter", NullWritable.get(), val, key + "/cnt1");

            }
            result.set(sum);
            context.write(key, result);


        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, LongAdder> entry : counter.entrySet()) {
                multipleOutputs.write("counter", entry.getKey(), entry.getValue().longValue(), "counter1/cnt");
            }
            counter = null;
            multipleOutputs.close();
            super.cleanup(context);
        }
    }
}