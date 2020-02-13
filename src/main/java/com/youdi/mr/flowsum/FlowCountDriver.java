package com.youdi.mr.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class FlowCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        // 1. 获取job对象
        Job job = Job.getInstance(conf);
        //2. 设置jar路径
        job.setJarByClass(FlowCountDriver.class);
        // 3. 设置mapper和reducer
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        // 4. 设置mapper输出value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 5.设置最终输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        // 设置分区
        job.setPartitionerClass(ProvincePartitioner.class);
        // 设置task
        job.setNumReduceTasks(4);

        // 6.设置输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        // 7. 提交job
        boolean b = job.waitForCompletion(true);

        System.exit(b ? 1 : 0);

    }

}
