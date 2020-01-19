package com.youdi.mr.kv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KVMapper extends Mapper<Text, Text, Text, IntWritable> {

    IntWritable v = new IntWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        // 1. 封装对象

        // 2. 写出
        context.write(key, v);

    }
}
