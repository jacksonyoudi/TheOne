package com.youdi.mr.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupCompartor extends WritableComparator {
    public OrderGroupCompartor() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 要求只要id相同， 就是一个key
        int result;
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;

        if (aBean.getOrder_id() > bBean.getOrder_id()) {
            result = 1;
        } else if (aBean.getOrder_id() < bBean.getOrder_id()) {
            return -1;
        } else {
            result = 0;
        }
        return result;
    }
}
