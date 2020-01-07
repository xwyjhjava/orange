/**
 * CasExample class
 *
 * @author ZhaoMing
 * @data 2019/12/8
 */
package com.dreams.thread.lock;

/**
 * 自旋锁
 *
 * Synchronized
 * 偏向锁 -> 自旋锁  ->  重量级锁
 *
 * 自旋锁 可以不经过内核态，适用场景：线程代码短，执行快
 */

public class CasExample {

    enum ReadyToRun{T1, T2}

    static volatile ReadyToRun r = ReadyToRun.T1;

    public static void main(String[] args) {


        char[] a1 = "123456".toCharArray();
        char[] a2 = "ABCDEF".toCharArray();

        new Thread(() -> {

            for (char c: a1) {
                while (r != ReadyToRun.T1) {}
                System.out.println(c);
                r = ReadyToRun.T2;
            }

        }, "t1").start();

        new Thread(() -> {

            for (char c: a2) {

                while(r != ReadyToRun.T2) {}
                System.out.println(c);
                r = ReadyToRun.T1;
            }


        }, "t2").start();


    }
}
