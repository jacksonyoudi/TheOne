package com.youdi.mr.index;

import com.youdi.mr.wordcount.WordcountDriver;
import com.youdi.mr.wordcount.WordcountMapper;
import com.youdi.mr.wordcount.WordcountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OneIndexDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/Users/youdi/Project/javaProject/TheOne/input", "/Users/youdi/Project/javaProject/TheOne/outputinex"};
        Configuration conf = new Configuration();

        // 1. job对象
        Job job = Job.getInstance(conf);

        // 2. 设置jar存储位置
        job.setJarByClass(OneIndexDriver.class);

        // 3. 关联map和reducer类
        job.setMapperClass(OneIndexMapper.class);
        job.setReducerClass(OndexReducer.class);

        // 4. 设置map阶段输出key和value类型

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. 设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        //  6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7. 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
