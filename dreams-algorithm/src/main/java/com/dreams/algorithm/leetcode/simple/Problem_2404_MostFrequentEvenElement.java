package com.dreams.algorithm.leetcode.simple;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Problem_2404_MostFrequentEvenElement {
    public static void main(String[] args) {

    }


    public int mostFrequentEven(int[] nums) {

        Map<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < nums.length; i++) {
            if (nums[i] % 2 == 0) {
                map.put(nums[i], map.getOrDefault(nums[i], 0) + 1);
            }
        }

        AtomicInteger numCount = new AtomicInteger(0);
        AtomicInteger result = new AtomicInteger(-1);
        // 遍历map
        map.forEach((key, value) -> {
            // 比较偶数次数
            if (value > numCount.get()) {
                result.set(key);
                numCount.set(value);
            }else if (value == numCount.get()) {
                if (key <= result.get()) {
                    result.set(key);
                }
            }
        });
        return result.get();
    }

}
