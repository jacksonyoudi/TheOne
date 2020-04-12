package com.bigdata.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class SecondSortApp {
    public static class MyMapper extends Mapper<Text, Text, Pair, IntWritable> {
        private final IntWritable v = new IntWritable();


        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            //StringTokenizer tokenizer = new StringTokenizer(value.toString());


            int first = Integer.parseInt(key.toString());
            int second = Integer.parseInt(value.toString());

            context.write(new Pair(first, second), new IntWritable(second));
        }
    }


    // 分组比较的时候，只比较原来的key，而不是组合key
    public static class GroupComparator implements RawComparator<Pair> {
        public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
            return WritableComparator.compareBytes(bytes, i, Integer.SIZE / 8, bytes1, i2, Integer.SIZE / 8);
        }

        public int compare(Pair o1, Pair o2) {
            return o1.getFirst() - o2.getFirst();
        }
    }

    public static class MyReducer extends Reducer<Pair, IntWritable, Text, IntWritable> {
        private final Text first = new Text();

        @Override
        protected void reduce(Pair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //context.write(SEPARATOR, null);
            first.set(Integer.toString(key.getFirst()));

            for (IntWritable value : values) {
                context.write(first, value);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        // 1. 获取job对象
        Job job = Job.getInstance(conf);
        //2. 设置jar路径
        job.setJarByClass(SecondSortApp.class);
        // 3. 设置mapper和reducer
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 4. 设置mapper输出value的类型
        job.setMapOutputKeyClass(Pair.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5.设置最终输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        //job.setInputFormatClass(TextInputFormat.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);


        // 分组函数
        job.setGroupingComparatorClass(GroupComparator.class);


        // 6.设置输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        // 7. 提交job
        boolean b = job.waitForCompletion(true);

        System.exit(b ? 1 : 0);
    }

}
