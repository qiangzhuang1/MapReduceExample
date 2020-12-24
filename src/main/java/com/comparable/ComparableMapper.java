package com.comparable;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ComparableMapper extends Mapper<LongWritable,Text, ComparableBean, NullWritable> {
    ComparableBean k = new ComparableBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] s = line.split(" ");
        // 设备Id
        k.setDeviceId(s[0]);
        // appkey
        k.setAppkey(s[1]);
        // 自有时长
        k.setSelfDuration(Long.valueOf(s[2]));
        context.write(k, NullWritable.get());
    }
}