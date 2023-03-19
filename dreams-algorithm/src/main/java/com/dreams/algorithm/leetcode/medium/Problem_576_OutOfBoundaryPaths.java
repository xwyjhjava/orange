package com.dreams.algorithm.leetcode.medium;

/**
 *
 */
public class Problem_576_OutOfBoundaryPaths {

    /**
     * There is an m x n grid with a ball. The ball is initially at the position [startRow, startColumn].
     * You are allowed to move the ball to one of the four adjacent cells in the grid (possibly out of the grid
     * crossing the grid boundary). You can apply at most maxMove moves to the ball.
     *
     * Given the five integers m, n, maxMove, startRow, startColumn, return the number of paths to move the ball
     * out of the grid boundary. Since the answer can be very large, return it modulo 109 + 7.
     *
     * 来源：力扣（LeetCode）
     * 链接：https://leetcode-cn.com/problems/out-of-boundary-paths
     *
     */

    public static int MOD = 1000000007;

    public static int findPath(int m, int n, int maxMove, int startRow, int startColumn){
        return 0;
    }

    // startRow -> [0, M]
    // startCol -> [0, N]

    // DFS， 该方法提交到Leetcode会超时，需要优化成动态规划
    public static int process(int m, int n, int maxMove, int startRow, int startColumn){

        // 球出界了, 找到一种方式
        if(startRow < 0 || startColumn < 0 || startRow >= m || startColumn >= n){
            return 1;
        }
        // 当maxMove == 0，表示此时无法再移动了
        if(maxMove == 0){
           return 0;
        }

        // 球在任意位置，都可以向四个方向移动
        int ans = process(m, n, maxMove - 1, startRow - 1, startColumn)
                + process(m, n, maxMove - 1, startRow, startColumn + 1)
                + process(m, n, maxMove - 1, startRow + 1, startColumn)
                + process(m, n, maxMove - 1, startRow, startColumn - 1);

        return ans % MOD;
    }




    public static void main(String[] args) {

        int m = 2, n = 2;
        int maxMove = 2;
        int startRow = 0, startColumn = 0;

        System.out.println("ans = " + process(m, n, maxMove, startRow, startColumn));

    }

}
