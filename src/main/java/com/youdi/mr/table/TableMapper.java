package com.youdi.mr.table;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    private String name;

    /**
     * mapper执行前的操作
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取文件名称 通过切片
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        this.name = inputSplit.getPath().getName();
    }

    TableBean tableRecord = new TableBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // id pid amount
        // pid pname

        String line = value.toString();
        String[] words = line.split(" ");
        if (this.name.startsWith("order")) { // 订单表
            tableRecord.setId(words[0]);
            tableRecord.setPid(words[1]);
            tableRecord.setAmount(Integer.parseInt(words[2]));
            tableRecord.setPname("");
            tableRecord.setFlag("order");
            k.set(words[1]);

        } else { // 产品表
            // pid pname
            tableRecord.setId("");
            tableRecord.setPid(words[0]);
            tableRecord.setAmount(0);
            tableRecord.setPname(words[1]);
            tableRecord.setFlag("pd");
            k.set(words[0]);
        }
        context.write(k, tableRecord);
    }
}
