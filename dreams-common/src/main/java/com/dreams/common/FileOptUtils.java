package com.dreams.common;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.List;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.common
 * @date 2020/4/3 16:34
 * @description 文件操作类
 */
public class FileOptUtils {


    public static File[] listFile(){
        File file = FileUtils.getFile("D:\\idea2019.3workspace\\dreams\\dreams-toutiao\\data");

        File[] files = file.listFiles();

        System.out.println(files.length);

        return files;
    }

    public static void main(String[] args) {

        File[] files = listFile();

        try {


            for (int i = 0; i < files.length; i++) {
                // 处理文本数据
                if (files[i].getName().contains(".txt")) {

                    List<String> lines = FileUtils.readLines(files[i], "UTF-8");
                    // 从第一行中获得标题
                    String title = lines.get(0);
                    System.out.println("title = " + title);
                    // 从第二行中获取发布人
                    String person = lines.get(1);
                    System.out.println("person = " + person);

                } else {  // html里多出一些图片或视频的链接信息， 现不处理

                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


}
