package com.youdi.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 */

public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);

        // 1 累计求和
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        // 2. 写出
        IntWritable v = new IntWritable(sum);
        context.write(key, v);
    }
}
