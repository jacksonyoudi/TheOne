package com.youdi.mr.table;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {


    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

        // 只有相同的key的才能到一起
        // 存放所有的订单集合
        ArrayList<TableBean> orderBeans = new ArrayList<TableBean>();

        // 存放产品信息
        TableBean pdBean = new TableBean();

        for (TableBean value : values) {
            if (value.getFlag().equals("order")) {
                try {
                    TableBean tmpBean = new TableBean();
                    // 做到值copy
                    BeanUtils.copyProperties(tmpBean, value);
                    orderBeans.add(tmpBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    BeanUtils.copyProperties(pdBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        for (TableBean orderBean : orderBeans) {
            orderBean.setPname(pdBean.getPname());
            context.write(orderBean, NullWritable.get());
        }


    }
}
