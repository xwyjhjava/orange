package com.dreams.designpattern.singleton;

/**
 * 双重检查
 */
public class Mgr06 {

    //volatile防止指令重排
    private static volatile Mgr06 INSTANCE;

    private Mgr06() {}

    public static Mgr06 getInstance(){
        if(INSTANCE == null){
            synchronized (Mgr06.class){
                if(INSTANCE == null){
                    INSTANCE = new Mgr06();
                }
            }
        }
        return INSTANCE;
    }

    public static void main(String[] args) {

    }
}
