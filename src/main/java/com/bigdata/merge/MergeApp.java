package com.bigdata.merge;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MergeApp {
    static class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
        private Text filenameKey;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 获取文件名
            InputSplit split = context.getInputSplit();
            FileSplit split1 = (FileSplit) split;
            Path path = split1.getPath();
            filenameKey = new Text(path.toString());
        }


        @Override
        protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            context.write(filenameKey, value);
        }
    }
}
