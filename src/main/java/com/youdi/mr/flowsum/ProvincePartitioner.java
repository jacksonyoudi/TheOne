package com.youdi.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    public ProvincePartitioner() {
        super();
    }

    @Override
    public int getPartition(Text key, FlowBean flowBean, int numPartitions) {
        // key手机号
        // value 流量

        // 获取前3位

        int partition = 4;
        String sNum = key.toString().substring(0, 3);
        if ("136".equals(sNum)) {
            partition = 0;
        } else if ("137".equals(sNum)) {
            partition = 1;
        } else if ("138".equals(sNum)) {
            partition = 2;
        } else {
            partition = 3;
        }

        return partition;
    }
}
