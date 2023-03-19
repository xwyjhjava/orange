package com.dreams.algorithm.dynamic;

public class FibonacciCore {


    // fibonacci : f(n) = f(n-1) + f(n-2) (n>=2)
    public static int process(int N){
        if(N == 0){
            return 0;
        }
        if(N == 1){
            return 1;
        }
        if(N == 2){
            return 1;
        }
        return process(N - 1) + process(N -2);
    }


    public static int process2(int N){
        if(N < 0){
            return -1;
        }

        int[] dp = new int[N + 1];
        dp[0] = 0;
        dp[1] = 1;
        dp[2] = 1;

        for (int index = 3; index < N + 1; index++) {
            dp[index] = dp[index - 1] + dp[index - 2];
        }

        return dp[N];
    }


    public static void main(String[] args) {
        System.out.println("ans = " + process(6));
        System.out.println("ans = " + process2(6));
    }
}
