package com.youdi.mr.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class CacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private String name;
    HashMap<String, String> pidMap = new HashMap<String, String>();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        this.name = inputSplit.getPath().getName();

        URI[] uris = context.getCacheFiles();
        String path = uris[0].getPath().toString();

        // 缓存小表
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            // 切割
            String[] words = line.split(" ");
            this.pidMap.put(words[0], words[1]);
        }
        IOUtils.closeStream(reader);
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (this.name.startsWith("order")) { // 订单表
            String[] words = line.split(" ");

            String pid = words[1];
            String pname = pidMap.get(pid);

            line = line + " " + pname;

            context.write(new Text(line), NullWritable.get());
        }
    }
}
