package com.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;


public class WordCountDriver{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取配置信息
        Configuration conf = new Configuration();
        if (args.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        conf.set("mapreduce.output.fileoutputformat.compress","true");
        conf.set("mapreduce.output.fileoutputformat.compress.type","RECORD");
        conf.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");

        //  封装任务
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCountDriver.class);

        // 设置mapper
        job.setMapperClass(WordCountMapper.class);

        // 设置reduce
        job.setReducerClass(WordCountReduce.class);

        // 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交并退出
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
