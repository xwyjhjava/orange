/**
 * testMain class
 *
 * @author ZhaoMing
 * @data 2019/12/8
 */
package com.dreams.thread.lock;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 自测
 */
public class testMain {

    Thread t1 = null, t2 = null;

    public void locksupport_test(){

        char[] a1 = "123456".toCharArray();
        char[] a2 = "ABCDEF".toCharArray();

        t1 = new Thread(() -> {

            for (char c:a1) {
                System.out.println(c);
                LockSupport.unpark(t2);
                LockSupport.park();
            }
        }, "t1");

        t2 = new Thread(() -> {

            for (char c:a2) {
                LockSupport.park();
                System.out.println(c);
                LockSupport.unpark(t1);
            }

        }, "t2");

        t1.start();
        t2.start();

    }

    public void lock_condition_test(){

        char[] a1 = "123456".toCharArray();
        char[] a2 = "ABCDEF".toCharArray();

        Lock lock = new ReentrantLock();
        Condition condition1 = lock.newCondition();
        Condition condition2 = lock.newCondition();

        new Thread(() -> {

            try {
                lock.lock();
                for (char c:a1) {
                    System.out.println(c);
                    condition2.signal();
                    condition1.await();
                }
                condition2.signal();
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                lock.unlock();
            }
        }, "t1").start();

        new Thread(() -> {

            try {
                lock.lock();
                for (char c:a2) {
                    System.out.println(c);
                    condition1.signal();
                    condition2.await();
                }
                condition1.signal();
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                lock.unlock();
            }


        }, "t2").start();

    }

    public static void main(String[] args) {

        new testMain().locksupport_test();

    }


}
