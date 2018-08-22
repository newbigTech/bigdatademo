package org.apache.hadoop;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class WordCount3 {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count2");
        job.setJarByClass(WordCount3.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileSystem fs = FileSystem.get(conf);
        Path outputpath = new Path("output2");
        FileOutputFormat.setOutputPath(job, outputpath);
        if (fs.exists(outputpath)) {
            fs.delete(outputpath, true);
        }
        FileInputFormat.addInputPath(job, new Path("input/*.txt"));
        job.waitForCompletion(true);
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String v = value.toString();
//            context.write(, one);
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, Text> {
        private IntWritable result = new IntWritable();
        private Map<String, LongAdder> counter;
        private List<String> words = Lists.newArrayList();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 初始化MultipleOutputs对象
            super.setup(context);
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            words.add(key.toString());
            if (words.size() >= 200) {
                System.out.println("call http request");
                Text text = new Text();
                text.set(StringUtils.join(words, ","));
                context.write(key, text);
                System.out.println(text.toString());
                words.clear();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("... cleanup");
            if (words.size() > 0) {
                Text text = new Text();
                System.out.println("call http request from cleanup");
                text.set(StringUtils.join(words, ","));
                context.write(new Text(words.get(0)), text);
                System.out.println(text.toString());
            }
            super.cleanup(context);
        }
    }
}