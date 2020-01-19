package com.youdi.mr.flowsum;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    Text k = new Text();
    FlowBean v = new FlowBean();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        //    1. 获取一行
        String line = value.toString();

        //    2.切割 \t
        String[] fields = line.split("\t");

        // 3. 封装对象
        k.set(fields[0]);
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);

        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);


        // 4. 写出
        context.write(k, v);
    }
}
