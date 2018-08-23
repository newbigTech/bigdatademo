package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Q10MiddlePersonsCountForComm extends Configured implements Tool {

    /**
     * 主方法，执行入口
     *
     * @param args 输入参数
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Q10MiddlePersonsCountForComm(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        // 实例化作业对象，设置作业名称
        Job job = Job.getInstance(getConf(), "Q10MiddlePersonsCountForComm");
        job.setJobName("Q10MiddlePersonsCountForComm");

        // 设置Mapper和Reduce类
        job.setJarByClass(Q10MiddlePersonsCountForComm.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        // 设置Mapper输出格式类
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 设置Reduce输出键和值类型
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
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

            // 输出key为0和value为员工编号+","+员工经理编号
            context.write(new IntWritable(0), new Text(kv[0] + "," + ("".equals(kv[3]) ? " " : kv[3])));
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, NullWritable, Text> {

        // 定义员工列表和员工对应经理Map
        List<String> employeeList = new ArrayList<String>();
        Map<String, String> employeeToManagerMap = new HashMap<String, String>();

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // 在reduce阶段把所有员工放到员工列表和员工对应经理Map中
            for (Text value : values) {
                employeeList.add(value.toString().split(",")[0].trim());
                employeeToManagerMap.put(value.toString().split(",")[0].trim(), value.toString().split(",")[1].trim());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int totalEmployee = employeeList.size();
            int i, j;
            int distance;
            System.out.println(employeeList);
            System.out.println(employeeToManagerMap);

            // 对任意两个员工计算出沟通的路径长度并输出
            for (i = 0; i < (totalEmployee - 1); i++) {
                for (j = (i + 1); j < totalEmployee; j++) {
                    distance = calculateDistance(i, j);
                    String value = employeeList.get(i) + " and " + employeeList.get(j) + " =                     " + distance;
                    context.write(NullWritable.get(), new Text(value));
                }
            }
        }

        /**
         * 该公司可以由所有员工形成树形结构，求两个员工的沟通的中间节点数，可以转换在员工树中两员工之间的距离
         * 由于在树中任意两点都会在某上级节点汇合，根据该情况设计了如下算法
         */
        private int calculateDistance(int i, int j) {
            String employeeA = employeeList.get(i);
            String employeeB = employeeList.get(j);
            int distance = 0;

            // 如果A是B的经理，反之亦然
            if (employeeToManagerMap.get(employeeA).equals(employeeB) || employeeToManagerMap.get(employeeB).equals(employeeA)) {
                distance = 0;
            }
            // A和B在同一经理下
            else if (employeeToManagerMap.get(employeeA).equals(
                    employeeToManagerMap.get(employeeB))) {
                distance = 0;
            } else {
                // 定义A和B对应经理链表
                List<String> employeeA_ManagerList = new ArrayList<String>();
                List<String> employeeB_ManagerList = new ArrayList<String>();

                // 获取从A开始经理链表
                employeeA_ManagerList.add(employeeA);
                String current = employeeA;
                while (!employeeToManagerMap.get(current).isEmpty()) {
                    current = employeeToManagerMap.get(current);
                    employeeA_ManagerList.add(current);
                }

                // 获取从B开始经理链表
                employeeB_ManagerList.add(employeeB);
                current = employeeB;
                while (!employeeToManagerMap.get(current).isEmpty()) {
                    current = employeeToManagerMap.get(current);
                    employeeB_ManagerList.add(current);
                }

                int ii = 0, jj = 0;
                String currentA_manager, currentB_manager;
                boolean found = false;

                // 遍历A与B开始经理链表，找出汇合点计算
                for (ii = 0; ii < employeeA_ManagerList.size(); ii++) {
                    currentA_manager = employeeA_ManagerList.get(ii);
                    for (jj = 0; jj < employeeB_ManagerList.size(); jj++) {
                        currentB_manager = employeeB_ManagerList.get(jj);
                        if (currentA_manager.equals(currentB_manager)) {
                            found = true;
                            break;
                        }
                    }

                    if (found) {
                        break;
                    }
                }

                // 最后获取两只之前的路径
                distance = ii + jj - 1;
            }

            return distance;
        }
    }
}
