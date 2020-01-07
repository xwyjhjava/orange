package com.dreams.designpattern.singleton;

/**
 *  懒汉式
 *  同步代码块
 *
 */
public class Mgr05 {

    private static Mgr05 INSTANCE;

    private Mgr05(){}

    public Mgr05 getInstance(){

        if(INSTANCE == null){

            synchronized (Mgr05.class){
                new Mgr05();
            }
        }
        return INSTANCE;
    }


}
