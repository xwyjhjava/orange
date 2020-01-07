/**
 * SyncExample class
 *
 * @author ZhaoMing
 * @data 2019/12/8
 */
package com.dreams.thread.lock;

import java.util.concurrent.CountDownLatch;

/**
 * wait and notified
 */
public class SyncExample {


    private static CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) {

        final Object o = new Object();

        char[] a1 = "123456".toCharArray();
        char[] a2 = "ABCDEF".toCharArray();

        new Thread(() -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            synchronized (o){
                for (char c: a1) {
                    System.out.println(c);
                    try {
                        o.notify(); //唤醒线程
                        o.wait();   //当前线程进入等待队列，让出锁
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                o.notify(); //必须， 否则无法停止程序
            }

        },"t1").start();

        new Thread(() -> {

            synchronized (o){
                for (char c:a2) {
                    System.out.println(c);
                    latch.countDown();
                    try {
                        o.notify();
                        o.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                o.notify();
            }

        }, "t2").start();

        //这里无法控制t1一定是在t2之前运行
        // TODO: 2019/12/8 如何控制线程先后顺序
        //用currentdownlatch、cas解决

    }

}
