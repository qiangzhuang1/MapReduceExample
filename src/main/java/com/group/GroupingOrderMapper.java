package com.group;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GroupingOrderMapper extends Mapper<LongWritable,Text, OrderBean, NullWritable> {
    OrderBean k = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] s = line.split(" ");
        // 订单id
        k.setOrderId(s[0]);
        // 金额
        k.setPrice(Double.valueOf(s[2]));
        context.write(k, NullWritable.get());
    }
}