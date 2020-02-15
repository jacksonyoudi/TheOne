package com.youdi.mr.sort;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    FlowBean k = new FlowBean();
    Text v = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1762222 2481 24455 27162

        String line = value.toString();
        String[] words = line.split(" ");
        String phone = words[0];
        long upFlow = Long.parseLong(words[1]);
        long downFlow = Long.parseLong(words[2]);
        // 序列化， 字段不能为空
        k.set(upFlow, downFlow);
        v.set(phone);

        context.write(k, v);
    }
}
