package com.youdi.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class SequenceFileDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        // 1.获取job对象
        Job job = Job.getInstance(conf);
        job.setJobName("sequence");

        // 2. 设置jar存储对象
        job.setJarByClass(SequenceFileDriver.class);

        // 3. 管理mapper和reducer类
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        // 4.设置mapper输出key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        // 5.设置最终输出key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);


        // 设置 文本输入class
        job.setInputFormatClass(WholeFileInputFormat.class);
        // 设置  输出
        job.setOutputFormatClass(SequenceFileOutputFormat.class);


        // 6. 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7. 提交job
        job.waitForCompletion(true);
    }
}
