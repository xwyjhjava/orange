/**
 * BlockingQueueExample class
 *
 * @author ZhaoMing
 * @data 2019/12/8
 */
package com.dreams.thread.lock;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class BlockingQueueExample {

    static BlockingQueue<String> q1 = new ArrayBlockingQueue<>(1);
    static BlockingQueue<String> q2 = new ArrayBlockingQueue<>(1);

    public static void main(String[] args) {
        char[] a1 = "123456".toCharArray();
        char[] a2 = "ABCDEF".toCharArray();

        new Thread(() -> {

            for (char c: a1) {
                System.out.println(c);
                try {
                    q1.put("ok");
                    q2.take();
                }catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        },"t1").start();


        new Thread(() -> {

            for (char c: a2) {
                try {
                    q1.take();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
                System.out.println(c);
                try{
                    q2.put("ok");
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        }, "t2").start();

    }
}
