package com.youdi.mr.website;

import com.youdi.mr.order.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WebSiteDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/Users/youdi/Project/javaProject/TheOne/input", "/Users/youdi/Project/javaProject/TheOne/output"};
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 1. 主
        job.setJarByClass(WebSiteDriver.class);
        job.setMapperClass(WebSiteMapper.class);
        job.setReducerClass(WebSiteReducer.class);

        //  map 2
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 最终输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(FilterOutputFormat.class);

        // 输出文件
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 虽然我们自定义了outformat，但是还有输出一个_success文件
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
