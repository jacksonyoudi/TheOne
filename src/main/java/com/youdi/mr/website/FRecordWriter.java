package com.youdi.mr.website;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream youdi;
    private FSDataOutputStream other;


    public FRecordWriter(TaskAttemptContext job) {

        // 1.获取文件系统
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            // 2 创建输出到youdi.log
            this.youdi = fs.create(new Path("/Users/youdi/Project/javaProject/TheOne/output/youdi.log"));

            // 创建输出到other.log
            this.other = fs.create(new Path("/Users/youdi/Project/javaProject/TheOne/output/other.log"));

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        // 判断
        if (key.toString().contains("youdi")) {
            this.youdi.write(key.toString().getBytes());
        } else {
            this.other.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(this.youdi);
        IOUtils.closeStream(this.other);
    }
}
