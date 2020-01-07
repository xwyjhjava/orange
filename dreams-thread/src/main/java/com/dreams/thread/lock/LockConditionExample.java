/**
 * LockConditionExample class
 *
 * @author ZhaoMing
 * @data 2019/12/8
 */
package com.dreams.thread.lock;


import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * lock\condition可以精确通知唤醒某一个线程
 */
public class LockConditionExample {



    public static void main(String[] args) {

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

}
