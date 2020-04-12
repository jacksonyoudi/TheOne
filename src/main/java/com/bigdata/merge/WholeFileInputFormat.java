package com.bigdata.merge;

import com.youdi.mr.inputformat.WholeRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import sun.jvm.hotspot.runtime.Bytes;

import java.io.IOException;
import java.io.SerializablePermission;
import java.util.List;

/**
 * 将整个文件作为一条记录处理的InputFormat
 */
public class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        WholeFileRecordReader reader = new WholeFileRecordReader();
        reader.initialize(inputSplit, taskAttemptContext);
        return reader;
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}


class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private FileSplit fileSplit;
    private Configuration conf;
    private BytesWritable value = new BytesWritable();
    private Boolean Processed = false;


    // 初始化
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 获取文件分片
        this.fileSplit = (FileSplit) inputSplit;
        // 获取配置
        this.conf = taskAttemptContext.getConfiguration();
    }

    // 读取下一个文件
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!Processed) {
            // 保存内容
            byte[] contents = new byte[(int) fileSplit.getLength()];
            // 获取路径
            Path path = fileSplit.getPath();

            // 获取文件系统
            FileSystem fs = path.getFileSystem(conf);
            FSDataInputStream in = fs.open(path);
            // 将文件数据写到content
            IOUtils.readFully(in, contents, 0, contents.length);
            value.set(contents, 0, contents.length);
            IOUtils.closeStream(in);
            Processed = true;
            return true;
        }
        return false;
    }

    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return Processed ? 1.0f : 0.0f;
    }

    public void close() throws IOException {
        // 关闭
    }
}