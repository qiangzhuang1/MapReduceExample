package com.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class PartitionDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"F:\\partitioninput", "F:\\partitionoutput"};
        // 获取配置信息
        Configuration conf = new Configuration();
        if (args.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        //  封装任务
        Job job = Job.getInstance(conf);
        job.setJarByClass(PartitionDriver.class);

        // 设置mapper
        job.setMapperClass(PartitionMapper.class);

        // 设置reduce
        job.setReducerClass(PartitionReduce.class);

        // 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PartitionBean.class);

        // 设置最终输出kv类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(PartitionBean.class);

        // 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置partitioner
        job.setPartitionerClass(CustomPartitioner.class);
        // reduceTask数量
        job.setNumReduceTasks(3);

        // 提交并退出
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}