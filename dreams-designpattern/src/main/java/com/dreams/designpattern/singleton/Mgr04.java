/**
 * Mgr04 class
 *
 * @author ZhaoMing
 * @data 2019/11/15
 */
package com.dreams.designpattern.singleton;

/**
 * 懒汉式
 *
 * 通过synchronized解决线程不安全的问题
 * 带来了效率降低
 */

public class Mgr04 {


    private static Mgr04 INSTANCE;

    private Mgr04(){}

    public synchronized static Mgr04 getInstance(){
        if(INSTANCE == null){
            INSTANCE = new Mgr04();
        }
        return INSTANCE;
    }


}
