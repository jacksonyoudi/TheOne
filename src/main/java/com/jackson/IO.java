package com.jackson;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class IO {
    public static void main(String[] args) {


        System.out.println("io流");
        Configuration conf = new Configuration();
        try {
            // 获取对象
            // 获取输入流
            // 获取输出流
            // 关闭资源
            FileSystem fs = FileSystem.get(new URI("hdfs://host1:9000"), conf, "root");
            FileInputStream fileInputStream = new FileInputStream(new File("/Users/youdi/Desktop/README.md"));


            FSDataOutputStream fsDataOutputStream = fs.create(new Path("/jackson/README.md"));

            IOUtils.copyBytes(fileInputStream, fsDataOutputStream, conf);
            IOUtils.closeStream(fsDataOutputStream);
            IOUtils.closeStream(fileInputStream);
            fs.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        System.out.println("ok");


    }


    public void testDownload() {

        System.out.println("io流");
        Configuration conf = new Configuration();
        try {
            // 获取对象
            // 获取输入流
            // 获取输出流
            // 关闭资源
            FileSystem fs = FileSystem.get(new URI("hdfs://host1:9000"), conf, "root");
            FSDataInputStream fis = fs.open(new Path(""));
            FileOutputStream fos = new FileOutputStream(new File(""));
            IOUtils.copyBytes(fis, fos, conf);

            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        System.out.println("ok");
    }


    // 下载第一块
    public void readFileSeek() {
        //1. 获取对象
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(new URI("hdfs://host1:9000"), conf, "root");

            // 2。 获取输入流
            FSDataInputStream fis = fs.open(new Path(""));
            FileOutputStream fos = new FileOutputStream(new File(""));

            byte[] buf = new byte[1024];

            for (int i = 0; i < 1024 * 128; i++) {
                fis.read(buf);
                fos.write(buf);
            }


            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);

            fs.close();


        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }


    }

    // 下载第一块
    public void readFileSeek2() {
        //1. 获取对象
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(new URI("hdfs://host1:9000"), conf, "root");

            // 2。 获取输入流
            FSDataInputStream fis = fs.open(new Path(""));
            FileOutputStream fos = new FileOutputStream(new File(""));

            byte[] buf = new byte[1024];

            // 3. 设置指定读取的起点
            fis.seek(1024 * 1024 * 128);


            // 流的对拷贝
            IOUtils.copyBytes(fis, fos, conf);

            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);

            fs.close();


        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }


    }


}
