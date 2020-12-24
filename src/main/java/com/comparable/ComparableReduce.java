package com.comparable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class ComparableReduce extends Reducer<ComparableBean, NullWritable, NullWritable, ComparableBean> {
    @Override
    protected void reduce(ComparableBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value :values) {
            context.write(value, key);
        }
    }
}
