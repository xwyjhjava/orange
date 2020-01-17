/**
 * MyWordCount class
 *
 * @author ZhaoMing
 * @data 2020/1/12
 */
package com.dreams.hadoop.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MyWordCount {

    public static void main(String[] args) throws IOException {


        Configuration conf = new Configuration(true);
        Job job = Job.getInstance(conf);

        //jar路径
        job.setJar("/xxx.jar");
        // 必须写，添加的自己运行的类
        job.setJarByClass(MyWordCount.class);
        // job 名称
        job.setJobName("word count");

        Path infile = new Path("/data/wc/input");
        TextInputFormat.addInputPath(job, infile);

        Path outfile = new Path("/data/wc/output");
        if (outfile.getFileSystem(conf).exists(outfile)) {
            outfile.getFileSystem(conf).delete(outfile, true);
        }
        TextOutputFormat.setOutputPath(job, outfile);

        List<Void> list = new ArrayList<>();


    }


}
