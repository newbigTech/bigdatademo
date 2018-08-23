package hadoop;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;

public class pu {
    public static void main(String[] args) throws Exception {
        //先获取目录下面的文件
        //然后读取出来放到Map
        //
        String uri = "output/counter1";
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create(uri), conf);
        FileStatus[] fs = hdfs.listStatus(new Path(uri));
        Path[] paths = FileUtil.stat2Paths(fs);
        Map<String, Long> counter = Maps.newHashMap();
        for (Path p : paths) {
            BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(p)));
            try {
                String line;
                while ((line = br.readLine()) != null) {
//                    System.out.println(line);
                    String[] ss = line.split("\t");
                    if (counter.containsKey(ss[0])) {
                        Long a = counter.get(ss[0]);
                        a = a + Long.valueOf(ss[1]);
                        counter.put(ss[0], a);
                    } else {
                        counter.put(ss[0], Long.valueOf(ss[1]));
                    }
                }
            } finally {
                br.close();
            }
        }
        for (Map.Entry<String, Long> entry : counter.entrySet()) {
            System.out.println(entry.getKey() + " === " + entry.getValue());
        }
    }
}
