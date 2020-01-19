package com.youdi.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// map阶段

/**
 * KEYIN  输入key
 * VALUEIN
 * KEYOUT
 * VALUEOUT
 */

public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);

        // 1. 获取一行
        String line = value.toString();

        // 2. 切割单词
        String[] words = line.split(" ");

        // 3循环写出

        Text k = new Text();
        IntWritable v = new IntWritable(1);
        for (String word : words) {

            k.set(word);
            context.write(k, v);
        }


    }
}
