package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Q6HigherThanAveSalary extends Configured implements Tool {

    /**
     * 主方法，执行入口
     *
     * @param args 输入参数
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Q6HigherThanAveSalary(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        // 实例化作业对象，设置作业名称
        Job job = Job.getInstance(getConf(), "Q6HigherThanAveSalary");
        job.setJobName("Q6HigherThanAveSalary");

        // 设置Mapper和Reduce类
        job.setJarByClass(Q6HigherThanAveSalary.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        // 必须设置Reduce任务数为1 # -D mapred.reduce.tasks = 1
        // 这是该作业设置的核心，这样才能够保证各reduce是串行的
        job.setNumReduceTasks(1);

        // 设置输出格式类
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 设置输出键和值类型
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 第1个参数为员工数据路径和第2个参数为输出路径
        String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }

    public static class MapClass extends Mapper<LongWritable, Text, IntWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // 对员工文件字段进行拆分
            String[] kv = value.toString().split(",");

            // 获取所有员工数据，其中key为0和value为该员工工资
            context.write(new IntWritable(0), new Text(kv[5]));

            // 获取所有员工数据，其中key为0和value为(该员工姓名 ,员工工资)
            context.write(new IntWritable(1), new Text(kv[1] + "," + kv[5]));
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {

        // 定义员工工资、员工数和平均工资
        private long allSalary = 0;
        private int allEmpCount = 0;
        private long aveSalary = 0;

        // 定义员工工资变量
        private long empSalary = 0;

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text val : values) {
                if (0 == key.get()) {
                    // 获取所有员工工资和员工数
                    allSalary += Long.parseLong(val.toString());
                    allEmpCount++;
                    System.out.println("allEmpCount = " + allEmpCount);
                } else if (1 == key.get()) {
                    if (aveSalary == 0) {
                        aveSalary = allSalary / allEmpCount;
                        context.write(new Text("Average Salary = "), new Text("" + aveSalary));
                        context.write(new Text("Following employees have salarys higher than Average:"), new Text(""));
                    }

                    // 获取员工的平均工资
                    System.out.println("Employee salary = " + val.toString());
                    aveSalary = allSalary / allEmpCount;

                    // 比较员工与平均工资的大小，输出比平均工资高的员工和对应的工资
                    empSalary = Long.parseLong(val.toString().split(",")[1]);
                    if (empSalary > aveSalary) {
                        context.write(new Text(val.toString().split(",")[0]), new Text("" + empSalary));
                    }
                }
            }
        }
    }
}
