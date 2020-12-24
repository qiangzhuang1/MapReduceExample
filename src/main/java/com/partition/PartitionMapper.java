package com.partition;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PartitionMapper extends Mapper<LongWritable,Text, Text, PartitionBean> {
    Text k = new Text();
    PartitionBean v = new PartitionBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] s = line.split(" ");
        // appkey合作硬件⼚商id
        k.set(s[1]);
        // 设备Id
        v.setDeviceId(s[0]);
        // appkey
        v.setAppkey(s[1]);
        // 自有时长
        v.setSelfDuration(Long.valueOf(s[2]));
        // 第三方时长
        v.setThirdPartDuration(Long.valueOf(s[3]));
        context.write(k, v);
    }
}