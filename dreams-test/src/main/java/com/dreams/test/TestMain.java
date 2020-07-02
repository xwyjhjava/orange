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



            ByteBuffer data = ByteBuffer.wrap(new byte[1024]);

            int index = 0;
            int count = 0;
            while ((index = socketChannel.read(data)) != -1){
                count ++;

                String s = new String(data.array());
                System.out.println("data = " + s);

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



    }


}
