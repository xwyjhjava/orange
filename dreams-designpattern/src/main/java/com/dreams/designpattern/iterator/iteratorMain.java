package com.dreams.designpattern.iterator;

/**
 * client的main方法
 */
public class iteratorMain {


    public static void main(String[] args) {

        ConcreteAggregate aggregate = new ConcreteAggregate();
        Concrete concrete1 = new Concrete();
        concrete1.setName("zhang");
        concrete1.setNo("1");
        aggregate.append(concrete1);

        Concrete concrete2 = new Concrete();
        concrete2.setNo("2");
        concrete2.setName("zhao");
        aggregate.append(concrete2);

        AIterator iterator = aggregate.createIterator();
        while(iterator.hasNext()){
            Concrete item = (Concrete) iterator.next();
            System.out.println("item = " + item.getNo() + "==" + item.getName());
        }

    }


}
