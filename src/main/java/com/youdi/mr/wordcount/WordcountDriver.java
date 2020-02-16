package com.youdi.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordcountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/Users/youdi/Project/javaProject/TheOne/input", "/Users/youdi/Project/javaProject/TheOne/output"};
        Configuration conf = new Configuration();

        // 1. job对象
        Job job = Job.getInstance(conf);

        // 2. 设置jar存储位置
        job.setJarByClass(WordcountDriver.class);

        // 3. 关联map和reducer类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        // 4. 设置map阶段输出key和value类型

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. 设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        // 设置maptasks
        //job.setNumReduceTasks(2);

        // 设置job
        //job.setCombinerClass(WordcountCombiner.class);
        job.setCombinerClass(WordcountReducer.class);


        //  6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);


        // 7. 提交job
        // job.submit();
        job.waitForCompletion(true);

    }
}
