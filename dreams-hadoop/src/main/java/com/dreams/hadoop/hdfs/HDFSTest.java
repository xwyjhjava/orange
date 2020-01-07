package com.dreams.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.hadoop.hdfs
 * @date 2020/1/7 14:20
 * @description hdfs测试
 */
public class HDFSTest {

    public Configuration conf = null;
    public FileSystem fs = null;

    @Before
    public void conn() throws IOException, InterruptedException {

        conf = new Configuration(true);
//        fs = FileSystem.get(conf);
        fs = FileSystem.get(URI.create("hdfs://mycluster"), conf, "root");

    }

    @Test
    public void mkdir() throws IOException{
        Path path = new Path("/test");
        if(fs.exists(path)){
            fs.delete(path, true);
        }
        fs.mkdirs(path);
    }

    @Test
    public void upload() throws IOException{
        BufferedInputStream stream = new BufferedInputStream(new FileInputStream(new File("D:\\xiaoi\\rec_min.txt")));
        Path path = new Path("/test/rec.txt");
        FSDataOutputStream outputStream = fs.create(path);

        IOUtils.copyBytes(stream, outputStream, conf, true);
    }
    @Test
    public void blocks() throws IOException{
        Path path = new Path("/test/rec.txt");
        FileStatus status = fs.getFileStatus(path);
        BlockLocation[] blks = fs.getFileBlockLocations(status, 0, status.getLen());
        for (BlockLocation b : blks){
            System.out.println("b = " + b);
        }
        FSDataInputStream in = fs.open(path);
        in.seek(104896);
        System.out.println((char)in.readByte());
    }

    @After
    public void close() throws IOException{
        fs.close();
    }


}
