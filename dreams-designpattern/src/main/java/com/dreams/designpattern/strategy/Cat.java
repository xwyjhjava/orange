package com.dreams.designpattern.strategy;

public class Cat implements Comparable{
    int height;
    int weight;

    public Cat(int height, int weight){
        this.height = height;
        this.weight = weight;
    }

    @Override
    public int compareTo(Object obj){
        Cat c = (Cat)obj;
        if(this.weight < c.weight) return -1;
        else if(this.weight > c.weight) return 1;
        else return 0;
    }
}
