package com.youdi.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        // 1.累加求和
        FlowBean v = new FlowBean();
        long sumUpFlow = 0;
        long sumDownFlow = 0;
        for (FlowBean value : values) {
            sumUpFlow += value.getUpFlow();
            sumDownFlow += value.getDownFlow();
        }
        v.set(sumUpFlow, sumDownFlow);

        // 2. 输出
        context.write(key, v);

    }
}
