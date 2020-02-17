package com.youdi.mr.mapjoin;

import com.youdi.mr.table.TableBean;
import com.youdi.mr.table.TableMapper;
import com.youdi.mr.table.TableReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class CaCheDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        args = new String[]{"/Users/youdi/Project/javaProject/TheOne/input", "/Users/youdi/Project/javaProject/TheOne/output"};
        Configuration conf = new Configuration();

        // 1. job对象
        Job job = Job.getInstance(conf);

        // 2. 设置jar存储位置
        job.setJarByClass(CaCheDriver.class);

        // 3. 关联map和reducer类
        job.setMapperClass(CacheMapper.class);
        job.setReducerClass(TableReducer.class);

        // 4. 设置map阶段输出key和value类型

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        // 5. 设置最终输出的key和value类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);


        // 加载缓存数据
        job.addCacheFile(new URI("/Users/youdi/Project/javaProject/TheOne/input/pd.txt"));

        // 取消reduce
        job.setNumReduceTasks(0);


        //  6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7. 提交job
        // job.submit();
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
