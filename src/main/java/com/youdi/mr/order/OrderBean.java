package com.youdi.mr.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private int order_id;
    private double price;

    public OrderBean() {
        super();
    }

    public OrderBean(int order_id, double price) {
        this.order_id = order_id;
        this.price = price;
    }

    public int compareTo(OrderBean o) {
        int flag = 0;
        if (this.order_id > o.getOrder_id()) {
            flag = 1;
        } else if (this.order_id < o.getOrder_id()) {
            flag = -1;
        } else {
            if (this.price > o.getPrice()) {
                flag = -1;
            } else if (this.price < o.getPrice()) {
                flag = 1;
            } else {
                flag = 0;
            }
        }
        return flag;
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.order_id);
        out.writeDouble(this.price);
    }

    public void readFields(DataInput in) throws IOException {
        this.order_id = in.readInt();
        this.price = in.readDouble();
    }

    @Override
    public String toString() {
        return order_id +
                " " + price;
    }
}
