package com.dreams.designpattern.singleton;

/**
 * 静态内部类
 *
 * JVM保证单例
 * 加载外部类时不会加载内部类
 *
 */
public class Mgr07 {

    public static class Mgr07Holder{
        private final static Mgr07 INSTANCE = new Mgr07();
    }

    public static Mgr07 getInstance(){
        return Mgr07Holder.INSTANCE;
    }

    public static void main(String[] args) {

    }

}
