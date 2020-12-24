package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class SpeakReduce extends Reducer<Text,SpeakBean, Text,SpeakBean> {
    @Override
    protected void reduce(Text key, Iterable<SpeakBean> values, Context context) throws IOException, InterruptedException {
        long selfDuration = 0;
        long thirdPartDuration = 0;
        // 遍历所⽤bean，将其中的自有，第三⽅时⻓分别累加
        for (SpeakBean sb : values) {
            selfDuration += sb.getSelfDuration();
            thirdPartDuration += sb.getThirdPartDuration();
        }
        // 封装对象
        SpeakBean resultBean = new SpeakBean(selfDuration, thirdPartDuration);
        // 写出
        context.write(key, resultBean);
    }
}
