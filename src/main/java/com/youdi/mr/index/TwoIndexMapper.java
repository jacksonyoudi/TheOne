package com.youdi.mr.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TwoIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //china--index.txt	6
        //china--order.txt	1
        //china--word.txt	2

        String line = value.toString();
        String[] words = line.split("--");
        k.set(words[0]);
        v.set(words[1]);
        context.write(k, v);
    }
}
