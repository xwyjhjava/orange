package com.dreams.algorithm.leetcode.medium;

import java.util.ArrayList;
import java.util.List;

public class Problem_1042_FlowerPlantingWithNoAdjacent {

    public static void main(String[] args) {
        int n = 5;
        int[][] paths = new int[][] {{1, 2}, {3, 4}};
        gardenNoAdj(n, paths);
    }

    public static int[] gardenNoAdj(int n, int[][] paths) {
        List<Integer>[] adj = new List[n];
        for (int i = 0; i < n; i++) {
            adj[i] = new ArrayList<>();
        }
        for (int[] path : paths) {
            // 花园 [1, 2] [3, 4]
            // adj[0].add(1)
            // adj[1].add(0)
            // adj[2].add(3)
            // adj[3].add(2)
            adj[path[0] - 1].add(path[1] - 1);
            adj[path[1] - 1].add(path[0] - 1);
        }
        int[] ans = new int[n];
        for (int i = 0; i < n; i++) {
            // boolean[] 的大小是 5 是因为题目中说了每个花园可以种植四种花之一，所以花的种类用 1、2、3、4 表示。
            // 为了方便起见，我们可以用 boolean[5] 来标记每种花是否已经被相邻的花园种植，其中 boolean[0] 是无用的，
            // 只是为了让下标和花的种类对应。例如，如果 boolean[2] 为 true，就表示有一个相邻的花园种植了 2 号花，
            // 那么当前花园就不能种植 2 号花。如果 boolean[2] 为 false，就表示没有相邻的花园种植了 2 号花，
            // 那么当前花园就可以种植 2 号花。
            boolean[] colored = new boolean[5];
            for (int vertex : adj[i]) {
                colored[ans[vertex]] = true;
            }
            for (int j = 1; j <= 4; j++) {
                if (!colored[j]) {
                    ans[i] = j;
                    break;
                }
            }
        }
        return ans;
    }
}
