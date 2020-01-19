package com.jackson;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFS {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        System.out.println("hadoop 创建目录");


        //1. 配置
        Configuration conf = new Configuration();

        conf.set("fs.defaultFs", "hdfs://h1:9000");

        //1. 获取hdfs客户端对象
        //FileSystem fileSystem = FileSystem.get(conf);

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://h1:9000"), conf, "root");


        String pathString = "/jackson";
        Path path = new Path(pathString);
        fileSystem.mkdirs(path);


        fileSystem.close();

        System.out.println("over");
    }

}
