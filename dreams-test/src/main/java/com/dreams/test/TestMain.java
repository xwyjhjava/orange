package com.dreams.test;

import io.netty.buffer.ByteBuf;
import org.junit.Test;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.test
 * @date 2020/1/9 10:55
 * @description 单元测试
 */
public class TestMain {

    @Test
    public void uriTest(){

        String path = "/xiaoi/recommend";
        try {
            URI uri = new URI(path);
            String schema = uri.getScheme();
            System.out.println("schema = " + schema);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        List<String> list = new ArrayList();

    }


    @Test
    public void replaceTest(){
        String str = "font-family: 等线; baba:qqqq;";
        String replaceStr = str.replaceAll("^[font\\-family](.*?)[$;]", "-");
        System.out.println("replaceStr = " + replaceStr);


    }


    @Test
    public void timeTest(){
        try {


            long start = System.currentTimeMillis();
            Thread.sleep(5000);
            long end = System.currentTimeMillis();
            long duration = end - start;
            System.out.println("duration = " + duration);

        }catch (Exception e){
            e.printStackTrace();
        }
    }



    @Test
    public void downloadTest(){

        try {

            String ip = "192.168.199.168";
            SocketAddress socketAddress = new InetSocketAddress(ip, 8000);

            SocketChannel socketChannel = SocketChannel.open(socketAddress);
//            socketChannel.configureBlocking(false);



            boolean flag = socketChannel.isConnected();
            System.out.println("socket status = " + flag);


            boolean finishConnect = socketChannel.finishConnect();
            System.out.println("finishConnect = " + finishConnect);


            StringBuffer sb = new StringBuffer();
            sb.append("GET /trace1.data HTTP/1.1\r\n");
            sb.append("Host: 192.168.199.168:8000\r\n");
//            sb.append("Connection: Keep-Alive\r\n");
            sb.append("\r\n");


            ByteBuffer paramsBuffer = ByteBuffer.allocate(1024);
            paramsBuffer.put(sb.toString().getBytes());
            paramsBuffer.flip();

            socketChannel.write(paramsBuffer);



            ByteBuffer data = ByteBuffer.wrap(new byte[2048]);

            int index;
            int count = 0;

            while ((index = socketChannel.read(data)) != -1){
                count ++;


//                System.out.println("index = " + index);
//                data.get(buff, 0, index);
//                String s = new String(buff, 0, index, "UTF-8");
//                System.out.println("data = " + s);


                data.flip();
                while (data.remaining() > 0){
                    System.out.println("data.get() = " + new String());
                }
                data.clear();



                if(count == 10){
                    break;

                }


            }



        }catch (Exception e){
            e.printStackTrace();
        }

    }


    @Test
    public void socketChannelTest(){

        try {
            SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress("192.168.199.168", 8000));
            boolean finishConnect = socketChannel.finishConnect();
            System.out.println("finishConnect = " + finishConnect);


            // 发送简单http请求==============
            StringBuffer sb = new StringBuffer();
            sb.append("GET /trace1.data HTTP/1.1\r\n");
            sb.append("Host: 192.168.199.168:8000\r\n");
//            sb.append("Connection: Keep-Alive\r\n");
            sb.append("\r\n");
            ByteBuffer paramsBuffer = ByteBuffer.allocate(1024);

            System.out.println("paramsBuffer.position() = " + paramsBuffer.position());  // 0
            System.out.println("paramsBuffer.limit() = " + paramsBuffer.limit()); // 1024

            paramsBuffer.put(sb.toString().getBytes());

            int position = paramsBuffer.position();
            int limit = paramsBuffer.limit();
            System.out.println("position = " + position); // 57
            System.out.println("limit = " + limit); // 1024

            // 调整下标， 数据可读
            paramsBuffer.flip();

            int after_flip_position = paramsBuffer.position();
            int after_flip_limit = paramsBuffer.limit();
            System.out.println("after_flip_position = " + after_flip_position); // 0
            System.out.println("after_flip_limit = " + after_flip_limit); // 57

            socketChannel.write(paramsBuffer);


            long maxMemory = Runtime.getRuntime().maxMemory();
            System.out.println("maxMemory = " + maxMemory);

            // 读取数据 ================================
            long freeMemory = Runtime.getRuntime().freeMemory();
            System.out.println("freeMemory = " + freeMemory);
            ByteBuffer buffer1 = ByteBuffer.allocate(10);  // 第一种 ： 分配在堆上， heap空间
            long freeMemory1 = Runtime.getRuntime().freeMemory();
            System.out.println("freeMemory1 = " + freeMemory1);
            ByteBuffer buffer2 = ByteBuffer.allocateDirect(1024); // 第二种： 分配在堆外， offheap空间
            long freeMemory2 = Runtime.getRuntime().freeMemory();
            System.out.println("freeMemory2 = " + freeMemory2);
            // 第三种： 分配在堆外， 内核可以直接访问

//            socketChannel.read(buffer1, 57, 1024);

//            int read = socketChannel.read(buffer1);
//            System.out.println("read = " + read);


            System.out.println("==================================");

            byte[] bytes = new byte[1024];

            int count = -1;
            while (socketChannel.read(buffer1) != -1){
                count ++;

                // 确定数据的起终点
                buffer1.flip();

                System.out.println("buffer1 = " + buffer1.toString());
                long freeMemory3 = Runtime.getRuntime().freeMemory();
                System.out.println("freeMemory3 = " + freeMemory3);

                System.out.println("-----------------------");

                if(count > 5){
                    break;
                }

                buffer1.clear();
            }




//            String data = new String(byteBuffer.array());
//            System.out.println("data = " + data);


        }catch (Exception e){
            e.printStackTrace();
        }


    }



    @Test
    public void nioTest(){



    }


}
