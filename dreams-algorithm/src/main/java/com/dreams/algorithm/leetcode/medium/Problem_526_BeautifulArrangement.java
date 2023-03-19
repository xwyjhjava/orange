package com.dreams.algorithm.leetcode.medium;

/**
 * Suppose you have n integers labeled 1 through n. A permutation of those n integers perm (1-indexed) is
 * considered a beautiful arrangement if for every i (1 <= i <= n), either of the following is true:
 *
 * perm[i] is divisible by i.
 * i is divisible by perm[i].
 * Given an integer n, return the number of the beautiful arrangements that you can construct.
 *
 * 来源：力扣（LeetCode）
 * 链接：https://leetcode-cn.com/problems/beautiful-arrangement
 *
 *
 */
public class Problem_526_BeautifulArrangement {


    public static int countArrangement(int n){
        if(n < 1 || n > 15){
            return 0;
        }
        return process(n,0, 0);

    }

    /**
     *
     * @param N [1, 15]
     * @return
     */

    // [i..N], 能够组成的优美队列
    // 从左往右尝试
    // i 代表 优美队列下标
    // j 代表候选数组下标
    public static int process(int N, int i, int j){
        //base case
        if(i == N){ // 此时是一种队列
            return 1;
        }
        if(i < N && j == N - 1){ // 此时无效解
            return 0;
        }
        // 普遍位置 i
        int ans = 0;
        // 符合优美
        if(i % (j + 1) == 0 || (j + 1) % i == 0){
            ans = 1 + process(N, i + 1, j + 1);
        }else{
            ans = process(N, i, j + 1);
        }
        return ans;
    }


}
