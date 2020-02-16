package com.youdi.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordcountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 1. 累加求和
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        // 写出
        context.write(key, new IntWritable(sum));
    }
}
