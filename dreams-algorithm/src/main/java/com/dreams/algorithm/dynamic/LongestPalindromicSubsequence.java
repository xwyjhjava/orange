package com.dreams.algorithm.dynamic;

/**
 * 最长回文子序列
 */
public class LongestPalindromicSubsequence {


    // 解法一： 生成逆序串，原串和逆序串的最长公共子序列长度即为最长回文子序列

    // 解法二:
    public static int longestPalindromeSubseq(String s){
        if(s == null || s.length() == 0){
            return 0;
        }
        char[] str = s.toCharArray();
        return process(str, 0, s.length() - 1);
    }

    // 在str[L..R]范围上，最长回文子序列
    public static int process(char[] str, int L, int R){
        if(L == R){
            return 1;
        }
        // 如果剩两个字符
        if(L == R - 1){
            return str[L] == str[R] ? 2 : 1;
        }

        // 四种可能性
        // （1）❌ L，❌ R
        // （2）✅ L，❌ R
        // （3）❌ L，✅ R
        // （4）✅ L，✅ R

        int p1 = process(str, L + 1, R - 1);
        int p2 = process(str, L, R - 1);
        int p3 = process(str, L + 1, R);
        int p4 = str[L] == str[R] ? (2 + process(str, L + 1, R - 1)) : 0;

        return Math.max(p1, Math.max(p2, Math.max(p3, p4)));

    }


    // ==========================================
    // 动态规划

    public static int process2(String s){
        if(s == null | s.length() == 0){
            return 0;
        }
        char[] str = s.toCharArray();
        int N = s.length();
        int[][] dp = new int[N][N];

        // L: 0...N-1
        // R: 0...N-1

        dp[N - 1][N - 1] = 1;
        for(int i = 0; i < N - 1; i++){
            dp[i][i] = 1;
            dp[i][i + 1] = str[i] == str[i + 1] ? 2 : 1;
        }

        // 从地下往上填
        for(int L = N - 3; L >= 0; L--){
            for(int R = L + 2; R < N; R++){
                int p1 = dp[L + 1][R - 1];
                int p2 = dp[L][R - 1];
                int p3 = dp[L + 1][R];
                int p4 = str[L] == str[R] ? (2 + dp[L + 1][R - 1]) : 0;
                dp[L][R] = Math.max(Math.max(p1, p2), Math.max(p3, p4));
            }
        }

        return dp[0][N - 1];
    }


    // ===========================================
    // 继续优化

    public static int process3(String s){
        if(s == null | s.length() == 0){
            return 0;
        }
        char[] str = s.toCharArray();
        int N = s.length();
        int[][] dp = new int[N][N];

        // L: 0...N-1
        // R: 0...N-1

        dp[N - 1][N - 1] = 1;
        for(int i = 0; i < N - 1; i++){
            dp[i][i] = 1;
            dp[i][i + 1] = str[i] == str[i + 1] ? 2 : 1;
        }

        // 从地下往上填
        // 左下的依赖是不需要的
        for(int L = N - 3; L >= 0; L--){
            for(int R = L + 2; R < N; R++){
                // 首先角逐出左和下的最大值
                dp[L][R] = Math.max(dp[L][R - 1], dp[L + 1][R]);
                // 如果有第四种情况，再角逐出最大值
                if(str[L] == str[R]){
                    dp[L][R] = Math.max(dp[L][R], 2 + dp[L + 1][R - 1]);
                }
            }
        }

        return dp[0][N - 1];
    }










    public static void main(String[] args) {

    }
}
