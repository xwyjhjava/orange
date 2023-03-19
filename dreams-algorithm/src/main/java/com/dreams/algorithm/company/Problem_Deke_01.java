package com.dreams.algorithm.company;

public class Problem_Deke_01 {

    // 动态规划
    // 当前轮可选可不不选，选的话是总分加上牌面分，不选的话总分还原为三轮前的分数
    // 当前轮次小于等于3，总分数置为0


    public static int topScore(int[] arr){
        if(arr == null || arr.length < 1){
            return 0;
        }

        return process(0, arr);


    }

    public static int process(int start, int[] arr){
        if(start == arr.length || start < 0){
            return 0;
        }
        // 选了当前牌
        int p1 = process(start + 1, arr) + arr[start];
        // 没选当前牌,分数取三轮前的分数
        int p2 = process(start - 3, arr);

        return Math.max(p1, p2);
    }


    public static void main(String[] args) {
        int[] arr = {1,-5,-6,4,3,6,-2};
        int ans = topScore(arr);
        System.out.println(ans);
    }

}
