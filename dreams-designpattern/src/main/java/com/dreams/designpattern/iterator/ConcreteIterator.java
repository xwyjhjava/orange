package com.dreams.designpattern.iterator;

public class ConcreteIterator extends AIterator{

    // 定义具体的聚集对象
    private ConcreteAggregate aggregate;
    private int index;

    public ConcreteIterator(ConcreteAggregate aggregate){
        this.aggregate = aggregate;
        this.index = 0;
    }

    @Override
    Concrete first() {
        return aggregate.getFirst();
    }

    @Override
    Concrete next() {
        Concrete concrete = aggregate.getCurrent(index);
        index++;
        return concrete;
    }

    @Override
    boolean hasNext() {
        if(index < aggregate.count()){
            return true;
        }else {
            return false;
        }
    }

    @Override
    Concrete currentItem() {
        Concrete concrete = aggregate.getCurrent(index);
        return concrete;
    }
}
