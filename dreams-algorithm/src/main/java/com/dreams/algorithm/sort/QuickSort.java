package com.dreams.algorithm.sort;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 快速排序
 */
public class QuickSort extends AbstractSort{


    private List<Integer> list = new ArrayList<>();
    private int[] array = new int[5];



    @Override
    public void setup() {
        super.setup();

        array[0] = 5;
        array[1] = 2;
        array[2] = 3;
        array[3] = 1;
        array[4] = 8;

    }


    /**
     * 总体方案：插入排序+快速排序；
     * 1)对快速排序进行改造，为了节约空间，防止另外开辟数组空间，采用原地交换的方式
     * 2)当递归进行到数组元素个数到阈值后,采用插入排序
     */


    //快速排序
    @Override
    public void sort() {

        //jdk内部实现是Dual-Pivot Quicksort
//        Arrays.sort(array);

        int left = 0;
        int right = array.length - 1;
        int length = array.length;

        for (int i = 0; i < length; i++) {
            int ai = array[i];


        }





    }

    public void printResult(){

        for (int ele: array){
            System.out.println(ele);
        }
    }


    public static void main(String[] args) {
        QuickSort quickSort = new QuickSort();
        quickSort.setup();
        quickSort.sort();
        quickSort.printResult();
    }
}
