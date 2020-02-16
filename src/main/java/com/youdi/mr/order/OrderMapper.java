package com.youdi.mr.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 pid_1 222.8
        String line = value.toString();
        String[] words = line.split(" ");
        int oid = Integer.parseInt(words[0]);
        double price = Double.parseDouble(words[2]);
        OrderBean orderBean = new OrderBean(oid, price);
        context.write(orderBean, NullWritable.get());
    }
}
