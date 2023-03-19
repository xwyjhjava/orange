package com.dreams.test;

import io.netty.buffer.ByteBuf;
import org.junit.Test;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Stack;


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



    @Test
    public void bubbleSortTest(){
        int[] a = {1, 4, 3, 7, 9};

        for(int i = 0; i < a.length; i++){
            for(int j = i + 1; j < a.length; j++){
                if(a[i] < a[j]){
                    int tmp = a[i];
                    a[i] = a[j];
                    a[j] = tmp;
                }
            }
        }


        for (int i = 0; i < a.length; i++) {
            System.out.println("i = " + a[i]);
        }


    }



    @Test
    public void truncateTime(){

        LocalDateTime now = LocalDateTime.now();
        LocalDateTime localDateTime = now.truncatedTo(ChronoUnit.MINUTES);

        System.out.println("localDateTime = " + localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));



    }


    @Test
    public void hivesubstring(){
        String logtime = "2019-07-15 00:00:00";
        String newstring = logtime.substring(0, 10);
        System.out.println("newstring = " + newstring);
    }



    @Test
    public void conpareTwoArray(){

//        List<String> A = new ArrayList<>();

        // 比较A 集合内与
        List<BStudent> A = new ArrayList<>();
        BStudent student = new BStudent();
        student.setName("悠米");
        student.setSex("公");
        A.add(student);

        BStudent student1 = new BStudent();
        student1.setName("测试");
        student1.setSex("母");
        A.add(student1);

        BStudent student7 = new BStudent();
        student7.setName("测试");
        student7.setSex("母");
        A.add(student7);


        List<BStudent> B = new ArrayList<>();
        BStudent student2 = new BStudent();
        student2.setName("悠米");
        student2.setSex("公");
        B.add(student2);

        BStudent student3 = new BStudent();
        student3.setName("测试");
        student3.setSex("母");
        B.add(student3);

        BStudent student4 = new BStudent();
        student4.setName("第二组");
        student4.setSex("男");
        B.add(student4);

        BStudent student5 = new BStudent();
        student5.setName("第二组");
        student5.setSex("女");
        B.add(student5);


        // 重写equlas方法，设定name作为关键区分
        List<BStudent> result = new ArrayList<>();

        // 循环遍历 List A
        for(int i = 0; i < A.size(); i++){
            BStudent cur = A.get(i);
            // 如果B中存在，则add到result中
            if(B.contains(cur)){
                result.add(cur);
            }

        }
    }


    @Test
    public void containsTest(){

        BStudent A = new BStudent();
        A.setSex("male");
        A.setName("zhangsan");

        BStudent B = new BStudent();
        B.setSex("female");
        B.setName("zhangsan");

        List<BStudent> students = new ArrayList<>();
        students.add(A);



    }

    @Test
    public void findTwoFactor(){
        List<A> aList = new ArrayList<>();
        List<B> bList = new ArrayList<>();

        //init
        A a0 = new A();
        a0.setNo(0);
        A a1 = new A();
        a1.setNo(1);
        A a2 = new A();
        a2.setNo(2);
        A a3 = new A();
        a3.setNo(3);

        aList.add(a0);
        aList.add(a1);
        aList.add(a2);
        aList.add(a3);

        B b0 = new B();
        b0.setNo(0);
        b0.setName("TTC");

        B b1 = new B();
        b1.setNo(1);
        b1.setName("TTC");

        B b2 = new B();
        b2.setNo(2);
        b2.setName("CCG");

        B b3 = new B();
        b3.setNo(3);
        b3.setName("CCG");

//        bList.add(b0);
//        bList.add(b1);
        bList.add(b2);
        bList.add(b3);

        List<String> result = new ArrayList<>();

        for(int i = 0; i < bList.size(); i+=2){ //循环B

            int index = i;
            int size = 0;
            for(int j = 0; j < aList.size(); j++){
                // 第一个B值
                A curA = aList.get(j);
                if(curA.getNo() == bList.get(index).getNo()){ // 如果B0在A中能找到，则直接去找B1
                    size++;
                    index++; // index会多加一次，取值时减一
                    j = -1;
                }
                if(size == 2){ //如果已经找全了两个，则直接取值, 跳出循环
                    result.add(bList.get(index - 1).getName());
                    break;
                }
            }

        }

        for(String ans : result){
            System.out.println(ans);
        }



    }

    @Test
    public void testCalendar(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = Calendar.getInstance();
    }
}

class A {
    private int no;

    public int getNo() {
        return no;
    }

    public void setNo(int no) {
        this.no = no;
    }
}

class B {
    private int no;
    private String name;

    public int getNo() {
        return no;
    }

    public void setNo(int no) {
        this.no = no;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}



class BStudent{
    private String name;
    private String sex;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    // 重写equals


    @Override
    public boolean equals(Object obj) {
        if(obj == null){
            return false;
        }
        BStudent student = (BStudent) obj;
        if(name.equals(student.name)){
            return true;
        }
        return false;
    }

}

