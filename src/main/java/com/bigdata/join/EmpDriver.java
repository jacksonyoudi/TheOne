package com.bigdata.join;

import com.youdi.mr.flowsum.FlowCountDriver;
import com.youdi.mr.flowsum.FlowCountMapper;
import com.youdi.mr.flowsum.FlowCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class EmpDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        // 1. 获取job对象
        Job job = Job.getInstance(conf);
        //2. 设置jar路径
        job.setJarByClass(EmpDriver.class);
        // 3. 设置mapper和reducer
        job.setMapperClass(EmpMapper.class);
        job.setReducerClass(EmpReducer.class);

        // 4. 设置mapper输出value的类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Emplyee.class);

        // 5.设置最终输出
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);


        //job.setInputFormatClass(TextInputFormat.class);
        //job.setInputFormatClass(KeyValueTextInputFormat.class);


        // 6.设置输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        // 7. 提交job
        boolean b = job.waitForCompletion(true);

        System.exit(b ? 1 : 0);
    }
}
