package com.youdi.mr.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwoIndexReducer extends Reducer<Text, Text, Text, Text> {
    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // one a.text 3

        StringBuffer sbuf = new StringBuffer();

        for (Text value : values) {
            sbuf.append(value.toString().replace("\t", "-->") + "\t");
        }
        v.set(sbuf.toString());
        context.write(key, v);
    }
}
