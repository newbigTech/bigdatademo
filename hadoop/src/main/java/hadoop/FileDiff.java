package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class FileDiff {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count3");
        job.setJarByClass(FileDiff.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(DiffReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BooleanWritable.class);
        FileSystem fs = FileSystem.get(conf);
        job.setNumReduceTasks(1);
        Path outputpath = new Path("output3");
        FileOutputFormat.setOutputPath(job, outputpath);
        if (fs.exists(outputpath)) {
            fs.delete(outputpath, true);
        }
        FileInputFormat.addInputPath(job, new Path("input/diff1_tmpbak.txt"));
        FileInputFormat.addInputPath(job, new Path("input/diff2.txt"));
        job.waitForCompletion(true);
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, BooleanWritable> {

        private final static BooleanWritable TRUE = new BooleanWritable(true);
        private final static BooleanWritable FALSE = new BooleanWritable(false);
        private String path;

        @Override
        protected void setup(Context context) {

            Path filePath = ((FileSplit) context.getInputSplit()).getPath();
            path = filePath.toString();
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (path.contains("tmpbak")) {
                context.write(value, FALSE);
            } else {
                context.write(value, TRUE);
            }
        }
    }

    public static class DiffReducer extends Reducer<Text, BooleanWritable, NullWritable, LongWritable> {
        private Set<Boolean> sb = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 初始化MultipleOutputs对象
            super.setup(context);
            System.out.println(context.getTaskAttemptID());
        }

        @Override
        public void reduce(Text key, Iterable<BooleanWritable> values,
                           Context context) throws IOException, InterruptedException {
            sb.clear();
            for (BooleanWritable v : values) {
                sb.add(v.get());
            }
            LongWritable text = new LongWritable();
            if (sb.size() == 1) {
                if (sb.contains(true)) {
                    text.set(Long.valueOf(key.toString()));
                    context.write(NullWritable.get(), text);
                } else {
                    text.set(-Long.valueOf(key.toString()));
                    context.write(NullWritable.get(), text);
                }
                System.out.println(text.get() + "--" + sb.size());
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
}