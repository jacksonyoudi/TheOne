package com.bigdata.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class EmpReducer extends Reducer<LongWritable, Emplyee, NullWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<Emplyee> values, Context context) throws IOException, InterruptedException {
        Emplyee dept = null;
        ArrayList<Emplyee> emplyees = new ArrayList<Emplyee>();

        for (Emplyee value : values) {
            if (value.getFlag() == 1) {
                Emplyee emplyee = new Emplyee(value);
                emplyees.add(emplyee);
            } else {
                Emplyee dept1 = new Emplyee(value);
            }
        }
        if (dept != null) {
            for (Emplyee emplyee : emplyees) {
                emplyee.setDeptName(dept.getDeptName());
                context.write(NullWritable.get(), new Text(emplyee.toString()));
            }
        }

    }
}
