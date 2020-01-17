package com.dreams.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.hadoop.util
 * @date 2020/1/9 13:57
 * @description Hadoop的操作类
 */
public class HadoopOpsUtil {


    public static FileSystem fs = null;
    public static Configuration conf;

    static {
        conf = new Configuration(true);
    }

    public static boolean removeDir(String dir){
        Path path = new Path(dir);
        try {
            //get 方法内部会判断是否是本地文件系统
            fs = FileSystem.get(path.toUri(), conf, "hadoop");
            return fs.delete(path, true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }


    public static void removeDir(String dfsURI,String dir) {
        Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        FileSystem hdfs;
        try {
            if("".equals(dfsURI)) {
                hdfs = FileSystem.get(hadoopConf);
            } else {
                hdfs = FileSystem.get(new java.net.URI(dfsURI), hadoopConf);
            }
//            hdfs.delete(new org.apache.hadoop.fs.Path(dir), true);
            String scheme = hdfs.getScheme();
            System.out.println("scheme = " + scheme);
        } catch(IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }


    public static void blocks(){
        try {

            Path path = new Path("hdfs://122.226.240.131:8020/ch-recommend/data/test");
            fs = FileSystem.get(path.toUri(), conf, "hadoop");
            FileStatus fileStatus = fs.getFileStatus(path);
            long blockSize = fileStatus.getBlockSize();
            System.out.println("blockSize = " + blockSize);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {

//        String path = "file:///D:\\xiaoi\\test";
//        removeDir(path);
        blocks();
        try {
            InetAddress localHost = InetAddress.getLocalHost();
            System.out.println("localHost = " + localHost);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}
