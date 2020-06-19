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


        try {
            URL url = new URL(urlPath);
            // 建立连接
            HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);

            int code = httpURLConnection.getResponseCode();

            System.out.println("code = " + code);

            if(code == 200) {
                System.out.println("连接建立成功");
                long lengthLong = httpURLConnection.getContentLengthLong();
                System.out.println("lengthLong = " + lengthLong);
                //分割文件
                int blockSize = (int) (lengthLong / threadCount);
                System.out.println("blockSize = " + blockSize);

                for (int i = 0; i < threadCount; i++) {

                    //start index
                    long startIndex = i * blockSize;
                    //end index
                    long endIndex = startIndex + blockSize - 1;
                    if(i == threadCount - 1){
                        endIndex = lengthLong;
                    }

                    System.out.println("----------");
                    System.out.println("startIndex = " + startIndex);
                    System.out.println("endIndex = " + endIndex);

                    Thread thread = new Thread(new DownloadThread(i, urlPath, startIndex, endIndex), "download=" + i);
                    thread.start();
                }

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
        System.out.println("threadName = " + threadName + "run");


        try {

            URL url = new URL(urlPath);

            HttpURLConnection connection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
            // 获取输入流
            InputStream inputStream = connection.getInputStream();

            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));



        }catch (Exception e){
            e.printStackTrace();
        }


    }
}
