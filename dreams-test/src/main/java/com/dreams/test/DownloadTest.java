package com.dreams.test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.test
 * @date 2020/6/19 11:31
 * @description 多线程下载分段下载测试
 */
public class DownloadTest {

    // 线程数
    private static int threadCount = 5;
    // 流地址
    private static String urlPath = "http://192.168.199.168:8000/trace1.data";


    public static void main(String[] args) {

        HttpURLConnection httpURLConnection = null;

        try {
            URL url = new URL(urlPath);
            // 建立连接
            httpURLConnection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);


            /**
             * 使用range方式的拉取， 耗时为1.2s
             * 不使用range方式的拉取，耗时为24.5s
             *
             * 两者性能差距约为24倍
             */
            httpURLConnection.setRequestProperty("range", "bytes=" + 0 + "-" + 411235825);
            httpURLConnection.connect();


            int code = httpURLConnection.getResponseCode();

            System.out.println("code = " + code);

            if(code == 206) {
                System.out.println("连接建立成功");
                long lengthLong = httpURLConnection.getContentLengthLong();
                System.out.println("lengthLong = " + lengthLong);
                //分割文件
                int blockSize = (int) (lengthLong / threadCount);
                System.out.println("blockSize = " + blockSize);

//                Thread thread = new Thread(new DownloadThread(0, urlPath, 0, lengthLong), "download");
//                thread.start();

//                 通过range的方式读取
                InputStream inputStream = httpURLConnection.getInputStream();

                byte[] bytes = new byte[1024];



//                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

                long lineCount = 0;
                int index = 0;
                long no = 0;

                long start = System.currentTimeMillis();
                System.out.println("start = " + start);
                while ((index = inputStream.read(bytes)) != -1){
//                    System.out.println("bytes = " + bytes.toString());
                    for (int i = 0; i < bytes.length; i++) {
//                        System.out.print(" = " + bytes[i]);
                        if(bytes[i] == 10){
                            lineCount ++;
                            System.out.println("check = " + lineCount);
                        }else{
                            no ++;
//                            System.out.println("no check = " + no);
                        }
                    }
//                    System.out.println("\n");

                }
                System.out.println("***************************");
                long end = System.currentTimeMillis();
                System.out.println("end = " + end);
                long interval = end - start;
                System.out.println("interval = " + interval);
                System.out.println("lineCount = " + lineCount);
                System.out.println("***************************");



//                for (int i = 0; i < threadCount; i++) {
//
//                    //start index
//                    long startIndex = i * blockSize;
//                    //end index
//                    long endIndex = startIndex + blockSize - 1;
//                    if(i == threadCount - 1){
//                        endIndex = lengthLong;
//                    }
//
//                    System.out.println("----------");
//                    System.out.println("startIndex = " + startIndex);
//                    System.out.println("endIndex = " + endIndex);
//
//                    Thread thread = new Thread(new DownloadThread(i, urlPath, startIndex, endIndex), "download=" + i);
//                    thread.start();
//                }

            }else{
                System.out.println("服务器响应超时");
            }


        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}

/**
 *  具体的下载线程
 */
class DownloadThread implements Runnable{


    private int threadId;

    private String urlPath;

    private long startIndex;

    private long endIndex;


    public DownloadThread(int threadId, String urlPath, long startIndex, long endIndex) {
        this.threadId = threadId;
        this.urlPath = urlPath;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();
        System.out.println("threadName = " + threadName + " run ");


        try {

            System.out.println("startIndex = " + startIndex);
            System.out.println("endIndex = " + endIndex);


            URL url = new URL(urlPath);

            HttpURLConnection connection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
            connection.setRequestProperty("range", "bytes=" + startIndex + "-" + endIndex);
            // 获取输入流
            InputStream inputStream = connection.getInputStream();

            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            long lineCount = 0;

            long start = System.currentTimeMillis();
            System.out.println("start = " + start);
            while (reader.readLine() != null){
                lineCount ++;
            }

            System.out.println("***************************");
            long end = System.currentTimeMillis();
            System.out.println("end = " + end);
            long interval = end - start;
            System.out.println("interval = " + interval);
            System.out.println("lineCount = " + lineCount);
            System.out.println("***************************");


        }catch (Exception e){
            e.printStackTrace();
        }


    }
}
