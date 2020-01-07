/**
 * LockSupport class
 *
 * @author ZhaoMing
 * @data 2019/12/8
 */
package com.dreams.thread.lock;

import java.util.concurrent.locks.LockSupport;

public class LockSupportExample {

    static Thread t1 = null, t2 = null;

    public static void main(String[] args) {
        char[] a1 = "123455".toCharArray();
        char[] a2 = "ABCDEF".toCharArray();

        t1 = new Thread(() -> {

            for (char c:a1) {
                System.out.println(c);
                LockSupport.unpark(t2);
                LockSupport.park();
            }

        }, "t1");

        t2 = new Thread(() -> {

            for (char c: a2) {
                LockSupport.park(); //线程阻塞
                System.out.println(c);
                LockSupport.unpark(t1);
            }

        }, "t2");

        t1.start();
        t2.start();

    }

}
