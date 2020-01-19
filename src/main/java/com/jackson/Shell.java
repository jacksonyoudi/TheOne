package com.jackson;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Shell {


    public static void main(String[] args) {
//        try {
//            testCopyFromLocalFile();
//        } catch (URISyntaxException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }


        rename();
    }

    // 1. 文件上传
    static void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        System.out.println("上传文件");

        // 配置
        Configuration entries = new Configuration();
//        entries.set("dfs.client.use.datanode.hostname", "true");
//        entries.set()

        // 获取文件系统对象
        FileSystem fs = FileSystem.get(new URI("hdfs://host1:9000"), entries, "root");


        //调用api
        fs.copyFromLocalFile(new Path("/Users/youdi/Desktop/README.md"), new Path("/jackson/two.md"));

        // 关闭资源
        fs.close();

        System.out.println("上传完成");

    }


    // 下载
    static void testDonwloadFile() {
        /*
        1. 获取对象
        2. 执行下载操作
        3. 关闭资源
         */
        System.out.println("下载文件");
        Configuration conf = new Configuration();

        try {
            FileSystem fs = FileSystem.get(new URI("hdfs://host1:9000"), conf, "root");
            fs.copyToLocalFile(new Path("/jackson/two.md"), new Path("./"));
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


    // 删除
    static void deleteDonwloadFile() {
        /*
        1. 获取对象
        2. 执行下载操作
        3. 关闭资源
         */
        System.out.println("删除文件");
        Configuration conf = new Configuration();

        try {
            FileSystem fs = FileSystem.get(new URI("hdfs://host1:9000"), conf, "root");
            fs.delete(new Path("/jackson/two.md"));
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


    // rename
    static void rename() {
        /*
        1. 获取对象
        2. 执行下载操作
        3. 关闭资源
         */
        System.out.println("renmae文件");
        Configuration conf = new Configuration();

        try {
            FileSystem fs = FileSystem.get(new URI("hdfs://host1:9000"), conf, "root");
//            fs.rename(new Path("/jackson/two.md"), new Path("/jackson/micro.md"));

            // 查看文件信息

            RemoteIterator<LocatedFileStatus> fsLists = fs.listFiles(new Path("/jackson"), true);

            while (fsLists.hasNext()) {
                LocatedFileStatus status = fsLists.next();
                System.out.println(status.getPath().getName());
                System.out.println(status.getPermission());
                System.out.println(status.getBlockSize());
                System.out.println(status.getLen());

                BlockLocation[] blockLocations = status.getBlockLocations();

                for (BlockLocation blockLocation : blockLocations) {
                    String[] hosts = blockLocation.getHosts();

                    for (String host : hosts) {
                        System.out.println("host " + host);
                    }

                }
            }


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


    // rename
    static void isFile() {
        /*
        1. 获取对象
        2. 执行下载操作
        3. 关闭资源
         */
        System.out.println("判断文件");
        Configuration conf = new Configuration();

        try {
            FileSystem fs = FileSystem.get(new URI("hdfs://host1:9000"), conf, "root");

            // 查看文件信息


            FileStatus[] fileStatuses = fs.listStatus(new Path("/jackson"));
            for (FileStatus fileStatus : fileStatuses) {
                if (fileStatus.isFile()) {
                    System.out.println("file:" + fileStatus.getPath().getName());
                } else {
                    System.out.println("path:" + fileStatus.getPath().getName());
                }

                fileStatus.isDirectory();
            }


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
}
