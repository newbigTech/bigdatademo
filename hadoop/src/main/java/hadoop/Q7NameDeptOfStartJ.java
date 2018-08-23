package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import hadoop.utils.CacheUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Q7NameDeptOfStartJ extends Configured implements Tool {

    /**
     * 主方法，执行入口
     *
     * @param args 输入参数
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Q7NameDeptOfStartJ(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        // 实例化作业对象，设置作业名称
        Job job = Job.getInstance(getConf(), "Q7NameDeptOfStartJ");
        job.setJobName("Q7NameDeptOfStartJ");

        // 设置Mapper和Reduce类
        job.setJarByClass(Q7NameDeptOfStartJ.class);
        job.setMapperClass(MapClass.class);

        // 设置输入格式类
        job.setInputFormatClass(TextInputFormat.class);

        // 设置输出格式类
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 第1个参数为缓存的部门数据路径、第2个参数为员工数据路径和第3个参数为输出路径
        String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
        job.addCacheFile(new Path(otherArgs[0]).toUri());
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }

    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

        // 用于缓存 dept文件中的数据
        private Map<String, String> deptMap = new HashMap<String, String>();
        private String[] kv;

        // 此方法会在Map方法执行之前执行且执行一次
        @Override
        protected void setup(Context context) {
            deptMap = CacheUtils.getCacheFromHDFS(context);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // 对员工文件字段进行拆分
            kv = value.toString().split(",");

            // 输出员工姓名为J开头的员工信息，key为员工姓名和value为员工所在部门名称
            if (kv[1].trim().startsWith("J")) {
                context.write(new Text(kv[1].trim()), new Text(deptMap.get(kv[7].trim())));
            }
        }
    }
}