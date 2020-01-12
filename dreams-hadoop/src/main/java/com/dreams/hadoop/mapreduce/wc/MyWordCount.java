/**
 * MyWordCount class
 *
 * @author ZhaoMing
 * @data 2020/1/12
 */
package com.dreams.hadoop.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class MyWordCount {

    public static void main(String[] args) throws IOException {


        Configuration conf = new Configuration(true);
        Job job = Job.getInstance(conf);

        // 必须写，添加的自己运行的类
        job.setJarByClass(MyWordCount.class);

        job.setJobName("word count");

        Path infile = new Path("/data/wc/input");
//        TextInputFormat.addInputPath(job, infile);




    }


}
