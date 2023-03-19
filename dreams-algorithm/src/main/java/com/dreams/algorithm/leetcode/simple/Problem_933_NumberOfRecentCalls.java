package com.dreams.algorithm.leetcode.simple;

import java.util.ArrayDeque;
import java.util.Queue;

public class Problem_933_NumberOfRecentCalls {

    public static void main(String[] args) {
        RecentCounter counter = new RecentCounter();
        counter.ping(10);
    }





}

class RecentCounter{

    Queue<Integer> queue;
    public RecentCounter() {
        queue = new ArrayDeque<>();
    }

    public int ping(int t) {
        // 入队
        queue.offer(t);
        // 队首小于t-3000， 弹出
        while (queue.peek() < t - 3000){
            queue.poll();
        }
        return queue.size();
    }
}

class RecentCounter2{

    int left,right;
    int[] times = new int[10001];
    public RecentCounter2() {
        left = 0;
        right = 0;
    }

    public int ping(int t) {
        times[right++] = t;
        while (times[left] < t - 3000){
            left++;
        }
        return right - left;
    }
}
