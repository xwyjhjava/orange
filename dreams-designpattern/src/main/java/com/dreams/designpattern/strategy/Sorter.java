package com.dreams.designpattern.strategy;

import java.util.Arrays;

public class Sorter<T> {

    public void sort(T[] arr, Comparator<T> comparator){
//        Arrays.sort(arr);

        comparator.compare(arr[0], arr[1]);
    }
}
