/**
 * Mgr03 class
 *
 * @author ZhaoMing
 * @data 2019/11/15
 */
package com.dreams.designpattern.singleton;

/**
 * 懒汉式，带来了线程不安全
 */
public class Mgr03 {

    private static Mgr03 INSTANCE;


    public static Mgr03 getInstance(){

        if(INSTANCE == null){
//            try{
//                Thread.sleep(1);
//            }catch (InterruptedException e){
//                e.printStackTrace();
//            }
            INSTANCE = new Mgr03();
        }

        return INSTANCE;
    }


    public static void main(String[] args) {

        for (int i = 0; i< 100; i++){

            new Thread(() -> {
                System.out.println(Mgr03.getInstance().hashCode());
            }).start();

        }

    }
}
