package com.dreams.algorithm.leetcode.medium;


import java.util.HashMap;
import java.util.Map;

/**
 *  题目描述：
 *      你有一个凸的 n 边形，其每个顶点都有一个整数值。给定一个整数数组 values ，其中 values[i] 是第 i 个顶点的值（即 顺时针顺序 ）。
 *      假设将多边形 剖分 为 n - 2 个三角形。对于每个三角形，该三角形的值是顶点标记的乘积，三角剖分的分数是进行三角剖分后所有
 *      n - 2 个三角形的值之和。
 */
public class Problem_1039_MinimumScoreTriangulationOfPolygon {

    public static void main(String[] args) {

        Solution solution = new Solution();
        int[] values = {3, 7, 4, 5};
        int result = solution.minScoreTriangulation(values);
        System.out.println("result = " + result);
    }
}

/**
 * 解法1： 记忆化搜索
 *
 */
class Solution{
    int n;
    int[] values;
    Map<Integer, Integer> memoryMap = new HashMap<>();

    public int minScoreTriangulation(int[] values) {
        this.n = values.length;
        this.values = values;
        return dp(0, n - 1);
    }

    public int dp(int i, int j) {
        // 不满足三角形的要求
        if (i + 2 > j) {
            return 0;
        }
        if (i + 2 == j) {
            return values[i] * values[i + 1] * values[j];
        }
        int key = i * n + j;
        // 记忆
        if (!memoryMap.containsKey(key)) {
            int ans = Integer.MAX_VALUE;
            for (int k = i + 1; k < j; k++) {
                ans = Math.min(ans, dp(i, k) + dp(k, j) + values[i] * values[k] * values[j]);
            }
            memoryMap.put(key, ans);
        }
        return memoryMap.get(key);
    }
}
