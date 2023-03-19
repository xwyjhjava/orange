package com.dreams.designpattern.singleton;

public class DCLSingleton {

    private DCLSingleton(){}

    private static volatile DCLSingleton INSTANCE;

    public static DCLSingleton getInstance(){
        if(INSTANCE == null){
            synchronized (DCLSingleton.class){
                if(INSTANCE == null){
                    INSTANCE = new DCLSingleton();
                }
            }
        }
        return INSTANCE;
    }
}
