package com.dreams.designpattern.iterator;

import java.util.ArrayList;
import java.util.List;

/**
 * Concrete集合类
 */
public class ConcreteAggregate extends Aggregate{

    private List<Concrete> items;

    ConcreteAggregate(){
        this.items = new ArrayList<>();
    }

    @Override
    AIterator createIterator() {
        return new ConcreteIterator(this);
    }

    public int count(){
        return items.size();
    }

    public void append(Concrete concrete){
        items.add(concrete);
    }

    public Concrete getCurrent(int index){
        return items.get(index);
    }

    public Concrete getFirst(){
        return items.get(0);
    }

}
