package com.dreams.designpattern.strategy;

/**
 * 策略模式，需要什么样的策略就可实现该接口进行实现
 * @param <T>
 */

public interface Comparator<T> {

    int compare(T o1, T o2);

}
