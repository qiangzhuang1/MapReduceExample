package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class SpeakDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"F:\\speakinput", "F:\\speakoutput"};
        // 获取配置信息
        Configuration conf = new Configuration();
        if (args.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        //  封装任务
        Job job = Job.getInstance(conf);
        job.setJarByClass(SpeakDriver.class);

        // 设置mapper
        job.setMapperClass(SpeakMapper.class);

        // 设置reduce
        job.setReducerClass(SpeakReduce.class);

        // 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SpeakBean.class);

        // 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SpeakBean.class);

        // 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交并退出
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
