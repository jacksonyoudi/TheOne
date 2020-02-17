package com.youdi.mr.log;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // 解析数据 context使用计数器
        boolean result = parseLog(line, context);
        if (!result) {
            return;
        }

        // 解析通过
        context.write(value, NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {
        String[] words = line.split(" ");
        if (words.length > 11) {
            context.getCounter("map", "ture").increment(1);
            return true;
        } else {
            context.getCounter("map", "false").increment(1);
            return false;
        }
    }
}
