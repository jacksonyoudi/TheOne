package com.youdi.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<FlowBean, Text> {
    public ProvincePartitioner() {
        super();
    }

    @Override
    public int getPartition(FlowBean key, Text value, int numPartitions) {

        int partion = 4;
        // 按照手机号 前3位排序
        String ph = value.toString().substring(0, 3);

        if ("136".equals(ph)) {
            partion = 0;
        } else if ("137".equals(ph)) {
            partion = 1;
        } else if ("138".equals(ph)) {
            partion = 2;
        } else if ("139".equals(ph)) {
            partion = 3;
        }
        return partion;
    }
}
