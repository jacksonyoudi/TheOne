package com.bigdata.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EmpMapper extends Mapper<LongWritable, Text, LongWritable, Emplyee> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(",");
        // 这里不做数据的检验
        Emplyee emplyee = new Emplyee();
        if (words.length > 3) {
            emplyee.setEmpNo(words[3]);
            emplyee.setName(words[4]);
            emplyee.setFlag(1);
            context.write(new LongWritable(Long.valueOf(emplyee.getEmpNo())), emplyee);
        } else {
            emplyee.setDeptNo(words[1]);
            emplyee.setDeptName(words[2]);
            emplyee.setFlag(2);
            context.write(new LongWritable(Long.valueOf(emplyee.getDeptNo())), emplyee);
        }
    }
}
