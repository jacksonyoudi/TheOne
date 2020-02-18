package com.youdi.mr.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.HashMap;

public class OneIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private String name;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        this.name = split.getPath().getName();
    }

    Text k = new Text();
    IntWritable v = new IntWritable(0);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");

        // 可以使用hashmap，但是考虑如果数据很大，就有问题了
        for (String word : words) {
            k.set(word + "--" + name);
            context.write(k, v);
        }
    }
}
