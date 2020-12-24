package com.group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class GroupingOrderDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"F:\\groupingOrderInput", "F:\\groupingOrderOutPut"};
        // 获取配置信息
        Configuration conf = new Configuration();
        if (args.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        //  封装任务
        Job job = Job.getInstance(conf);
        job.setJarByClass(GroupingOrderDriver.class);

        // 设置mapper
        job.setMapperClass(GroupingOrderMapper.class);

        // 设置reduce
        job.setReducerClass(GroupingOrderReduce.class);

        // 设置map输出
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 设置最终输出kv类型
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 指定分区器
        job.setPartitionerClass(CustomPartitioner.class);

        //指定分组比较器
        job.setGroupingComparatorClass(CustomGroupingComparator.class);

        // 设置reduceTask梳理
        job.setNumReduceTasks(2);
        // 提交并退出
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}