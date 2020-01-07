package com.dreams.designpattern.strategy;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) {
//        int[] a = {9, 2, 3, 5, 7, 1, 4};

        Dog[] a = {new Dog(3), new Dog(5), new Dog(1)};
        Sorter<Dog> sorter = new Sorter();
        sorter.sort(a, new DogComparator());
        System.out.println(Arrays.toString(a));

    }
}
