package com.youdi.mr.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    // 无参构造器
    public FlowBean() {
        super();
    }

    public FlowBean(long upFlow, long downFlow) {
        super();

        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = this.upFlow + this.downFlow;
    }

    // 比较
    public int compareTo(FlowBean bean) {
        // 核心比较
        if (this.sumFlow > bean.getSumFlow()) {
            return -1;
        } else if (this.sumFlow < bean.getSumFlow()) {
            return 1;
        }

        return 0;
    }

    // 序列化
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    // 反序列化
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();

    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return upFlow +
                "\t" + downFlow +
                "\t" + sumFlow;
    }


    public void set(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = this.upFlow + this.downFlow;
    }
}
