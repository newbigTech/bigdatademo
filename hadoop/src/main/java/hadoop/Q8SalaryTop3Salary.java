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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Q8SalaryTop3Salary extends Configured implements Tool {

    /**
     * 主方法，执行入口
     *
     * @param args 输入参数
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Q8SalaryTop3Salary(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        // 实例化作业对象，设置作业名称
        Job job = Job.getInstance(getConf(), "Q8SalaryTop3Salary");
        job.setJobName("Q8SalaryTop3Salary");

        // 设置Mapper和Reduce类
        job.setJarByClass(Q8SalaryTop3Salary.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 设置输入格式类
        job.setInputFormatClass(TextInputFormat.class);

        // 设置输出格式类
        job.setOutputKeyClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputValueClass(Text.class);

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
            // 输出key为0和value为员工姓名+","+员工工资
            context.write(new IntWritable(0), new Text(kv[1].trim() + "," + kv[5].trim()));
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // 定义工资前三员工姓名
            String empName;
            String firstEmpName = "";
            String secondEmpName = "";
            String thirdEmpName = "";

            // 定义工资前三工资
            long empSalary = 0;
            long firstEmpSalary = 0;
            long secondEmpSalary = 0;
            long thirdEmpSalary = 0;

            // 通过冒泡法遍历所有员工，比较员工工资多少，求出前三名
            for (Text val : values) {
                empName = val.toString().split(",")[0];
                empSalary = Long.parseLong(val.toString().split(",")[1]);

                if (empSalary > firstEmpSalary) {
                    thirdEmpName = secondEmpName;
                    thirdEmpSalary = secondEmpSalary;
                    secondEmpName = firstEmpName;
                    secondEmpSalary = firstEmpSalary;
                    firstEmpName = empName;
                    firstEmpSalary = empSalary;
                } else if (empSalary > secondEmpSalary) {
                    thirdEmpName = secondEmpName;
                    thirdEmpSalary = secondEmpSalary;
                    secondEmpName = empName;
                    secondEmpSalary = empSalary;
                } else if (empSalary > thirdEmpSalary) {
                    thirdEmpName = empName;
                    thirdEmpSalary = empSalary;
                }
            }

            // 输出工资前三名信息
            context.write(new Text("First employee name:" + firstEmpName), new Text("Salary:" + firstEmpSalary));
            context.write(new Text("Second employee name:" + secondEmpName), new Text("Salary:" + secondEmpSalary));
            context.write(new Text("Third employee name:" + thirdEmpName), new Text("Salary:" + thirdEmpSalary));
        }
    }
}
