package com.youdi.mr.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // 找出最大的 只输出了第一个
        //context.write(key, NullWritable.get());

        // 输出所有了
        //for (NullWritable value : values) {
        //    context.write(key, NullWritable.get());
        //}

        // 输出topn
        int topn = 0;
        for (NullWritable value : values) {
            context.write(key, NullWritable.get());
            topn += 1;
            if (topn == 2) {
                break;
            }
        }
    }
}
