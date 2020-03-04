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
    public void setup() {
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


    // 冒泡排序， 从小到大
    @Override
    public void sort(){

        // 数组遍历
        //不需要遍历最后一个数字, 所以需要遍历的长度是数组长度 - 1
        for (int i = 0; i < data.size(); i++) {
            //内层循环， 从第二个数字开始
            for (int j = i + 1; j < data.size(); j++) {
                int temp = 0;
                //比较第一个数和第二个数
                //若前一个比后一个大， 则交换位置
                if(data.get(i) > data.get(j)){
                    temp = data.get(i);
                    data.set(i, data.get(j));
                    data.set(j, temp);
                }
            }
        }


    }

    public void printResult(){
        data.stream().forEach((ele) -> System.out.println(ele));
    }

    public static void main(String[] args) {
        BubbleSort sort = new BubbleSort();
        sort.setup();
        sort.sort();
        System.out.println("=======================");
        sort.printResult();
    }


}
