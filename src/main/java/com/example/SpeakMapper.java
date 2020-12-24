package com.example;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeakMapper extends Mapper<LongWritable,Text, Text,SpeakBean> {
    Text k = new Text();
    SpeakBean v = new SpeakBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] s = line.split(" ");
        // 设备ID
        k.set(s[0]);
        // 自有时长
        v.setSelfDuration(Long.valueOf(s[2]));
        // 第三方时长
        v.setThirdPartDuration(Long.valueOf(s[3]));
        context.write(k, v);
    }
}
