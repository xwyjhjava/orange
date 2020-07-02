package com.dreams.test;

import java.io.*;
import java.net.*;
import java.nio.channels.ByteChannel;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.test
 * @date 2020/6/25 13:22
 * @description IO写内存
 */
public class MenBuffTest {


    private static String URL_PATH = "http://192.168.199.168:8000/trace1.data";

    private static int port = 8000;



    public static void main(String[] args) {

        try {


            URL url = new URL(URL_PATH);
            String host = url.getHost();
            System.out.println("host = " + host);
            int port = url.getPort();
            System.out.println("port = " + port);


            Socket socket = new Socket(host, port);

            OutputStream outputStream = socket.getOutputStream();

            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
            StringBuffer sb = new StringBuffer();
            sb.append("GET /trace1.data HTTP/1.1\r\n");
            sb.append("Host: 192.168.199.168:8000\r\n");
//            sb.append("Connection: Keep-Alive\r\n");
            sb.append("\r\n");

            writer.write(sb.toString());
            writer.flush();


            InputStream inputStream = socket.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            int count = 0;
            long freeMemory = Runtime.getRuntime().freeMemory();
            System.out.println("freeMemory = " + freeMemory);
            System.out.println("======================================");
            while (reader.readLine() != null){
//                System.out.println(reader.readLine());
                count ++ ;
                long free = Runtime.getRuntime().freeMemory();
                System.out.println("free = " + free);
                String line = reader.readLine();
                String[] array = line.split("\\|");

            }


            System.out.println("count = " + count);

        } catch (IOException e) {
            e.printStackTrace();
        }


    }





}
