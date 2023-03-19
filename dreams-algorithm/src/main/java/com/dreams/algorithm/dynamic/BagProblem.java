package com.dreams.algorithm.dynamic;

/**
 * 背包问题 （从左往右的模型）
 */
public class BagProblem {


    /**
     *
     * @param w 所有的货重量
     * @param v 所有的货价值
     * @param bag 背包容量，不能超过这个载重
     * @return 不超重的情况下，能够得到的最大价值
     */
    public static int maxValue(int[] w, int[] v, int bag){
        if(w == null || v == null || w.length < 1 || v.length < 1 || w.length != v.length){
            return 0;
        }

        // 尝试
        return process1(w, v, 0, bag) ;
    }

    // 当前考虑到了 index 号货物， index之后的所有货物可以自由选择
    // 做的选择不能超过背包容量
    // 返回最大价值
    public static int process1(int[] w, int[] v, int index, int bag){
        //背包容量不够
        if(bag < 0){
            return -1;
        }
        // 没有货时
        if(index == w.length){
            return 0;
        }
        // 有货。index位置的货
        // bag有空间
        // 不要当前的货
        int p1 = process1(w, v, index + 1, bag);

        // 要当前的货
        int p2 = 0;
        int next = process1(w, v, index + 1, bag - w[index]);
        if(next != -1){
            p2 = v[index] + next;
        }
        return Math.max(p1, p2);
    }



    // =================================================
    // 动态规划

    /**
     *
     * @param w
     * @param v
     * @param index    [0, w.length)
     * @param bagRest  (0,  bag]
     * @return
     */
    public static int process2(int[] w, int[] v, int index, int bagRest){
        if(w == null || v == null || w.length < 1 || v.length < 1 || w.length != v.length){
            return 0;
        }

        int N = w.length;
        // 因为 index可以取到边界值N，所以dp数组的行设为N+1
        int[][] dp = new int[N + 1][bagRest + 1];

        for(int col = N - 1; col >= 0; col--){
            for(int rest = 0; rest <= bagRest; rest++){

                int p1 = dp[col + 1][rest];
                int p2 = -1;

                if(rest - w[col] >= 0){
                    p2 = v[col] + dp[col + 1][rest - w[col]];
                }
                dp[col][rest] = Math.max(p1, p2);
            }
        }
        return dp[index][bagRest];
    }







    public static void main(String[] args) {
        int[] weights = {3, 2, 4, 7};
        int[] values = {5, 6, 3, 19};
        int bag = 11;
        System.out.println("bag ans = " + maxValue(weights, values, bag));
        System.out.println("bag ans = " + process2(weights, values, 0, bag));
    }
}
