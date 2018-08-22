package org.apache.hadoop.utils;

import com.google.common.collect.Maps;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class CacheUtils {

    public static Map<String, String> getCacheFromHDFS(Mapper.Context context) {
        BufferedReader in = null;
        Map<String, String> deptMap = Maps.newHashMap();
        try {

            // 从当前作业中获取要缓存的文件
            URI[] paths = context.getCacheFiles();
            String deptIdName = null;
            for (URI path : paths) {

                // 对部门文件字段进行拆分并缓存到deptMap中
                if (path.toString().contains("dept")) {
                    in = new BufferedReader(new FileReader(path.toString()));
                    while (null != (deptIdName = in.readLine())) {

                        // 对部门文件字段进行拆分并缓存到deptMap中
                        // 其中Map中key为部门编号，value为所在部门名称
                        String[] ss = deptIdName.split(",");
                        deptMap.put(ss[0], ss[1]);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return deptMap;
    }
}
