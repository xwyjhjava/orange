package com.dreams.algorithm.leetcode.simple;

public class Problem_2373_LargestLocalValues {

    public int[][] largestLocal(int[][] grid) {
        int n = grid.length;
        int[][] maxLocal = new int[n-2][n-2];
        for (int i = 0; i < n - 2; i++) {
            for (int j = 0; i < n - 2; j++) {
                maxLocal[i][j] = maxValue(i, j, grid);
            }
        }
        return maxLocal;
    }

    public int maxValue(int i, int j, int[][] grid) {
        int max = grid[i][j];
        for (int k = i - 1; k < i + 2; k++) {
            for (int p = j - 1; p < j + 2; p++) {
                max = Math.max(max, grid[k][p]);
            }
        }
        return max;
    }


    public static void main(String[] args) {

    }
}
