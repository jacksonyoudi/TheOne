package com.jackson.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

public class TestCompress {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        // 压缩
        //compress("/Users/youdi/Downloads/movie/one.mp4", "org.apache.hadoop.io.compress.BZip2Codec");
        //compress("/Users/youdi/Downloads/movie/one.mp4", "org.apache.hadoop.io.compress.GzipCodec");
        compress("/Users/youdi/Downloads/movie/one.mp4", "org.apache.hadoop.io.compress.DefaultCodec");

        uncompress("/Users/youdi/Downloads/movie");
    }

    private static void uncompress(String fileName) throws IOException {
        // 1. 压缩方式检测
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(fileName));

        if (codec == null) {
            System.out.println("can not uncompress");
            return;
        }

        FileInputStream fis = new FileInputStream(new File(fileName));
        CompressionInputStream cis = codec.createInputStream(fis);

        FileOutputStream fos = new FileOutputStream(new File(fileName + ".decode"));

        IOUtils.copyBytes(cis, fos, 1024 * 1024, false);

        IOUtils.closeStream(fos);
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fis);

    }

    private static void compress(String fileName, String method) throws IOException, ClassNotFoundException {
        // 获取输入流
        FileInputStream fis = new FileInputStream(new File(fileName));

        Class classCodec = Class.forName(method);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(classCodec, new Configuration());
        FileOutputStream fos = new FileOutputStream(new File(fileName + codec.getDefaultExtension()));

        CompressionOutputStream cos = codec.createOutputStream(fos);

        IOUtils.copyBytes(fis, cos, 1024 * 1024, false);

        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);


    }


}
