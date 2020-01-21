package com.dreams.algorithm.sort;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.sort
 * @date 2020/1/21 17:59
 * @description 冒泡排序
 */
public class BubbleSort extends AbstractSort {


    private List<Integer> data = new ArrayList<>();



    @Override
    public void sort() {
        super.setup();

        data.add(1);
        data.add(3);
        data.add(2);
        data.add(17);
        data.add(69);
        data.add(23);
        data.add(18);
        data.add(30);
        data.add(5);
    }





    public static void main(String[] args) {

    }


}
