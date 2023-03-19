package com.dreams.designpattern.iterator;

/**
 * 抽象迭代器类
 * 定义迭代的方法，如：First()、Next()、IsDone()、CurrentItem()
 */
abstract public class AIterator {

    // 开始对象
    abstract Object first();
    // 下一个对象
    abstract Object next();
    // 是否到结尾
    abstract boolean hasNext();
    // 当前对象
    abstract Object currentItem();
}
