package com.dreams.algorithm.dynamic;


/**
 * 样本对应模型
 *
 * 最长公共子序列长度
 *
 */
public class LongestCommonSubsequence {



    public static int longestSubsequence(String s1, String s2){
        if(s1 == null || s2 == null || s1.length() == 0 || s2.length() == 0){
            return 0;
        }
        char[] str1 = s1.toCharArray();
        char[] str2 = s2.toCharArray();

        // 尝试
        return process(str1, str2, str1.length - 1, str2.length - 1);

    }


    // str1上的[0, i]和str2[0,j]上的最长公共子序列多长
    // 从右往左看
    public static int process(char[] str1, char[] str2, int i, int j){
        // 四种情况
        // (1). 当 i == 0 && j == 0时，即str1和str2的范围都只剩一个字符
        if(i == 0 && j == 0){
            return str1[i] == str2[j] ? 1 : 0;
        }else if(i == 0){ // (2). 当 i == 0时，即str1的范围只剩一个字符
            if(str1[i] == str2[j]){ //表示str2上能找到一个和str1相同的字符串
                return 1;
            }else{ // 否则就在[0, j - 1]范围上再进行尝试
                return process(str1, str2, i, j - 1);
            }
        }else if(j == 0){ // (3). 当 j==0时， 即str2的范围只剩下一个字符
            if(str1[i] == str2[j]){
                return 1;
            }else{
                return process(str1, str2, i - 1, j);
            }
        }else{ // (4). 当i、j都不为0时，即str1和str2的范围都不止一个字符

            // 考虑以i作为结尾，不可能考虑j
            int p1 = process(str1, str2, i, j - 1);
            // 考虑以j作为结尾，不可能考虑i
            int p2 = process(str1, str2, i - 1, j);
            // 考虑i、j结尾
            int p3 = str1[i] == str2[j] ? (1 + process(str1, str2, i - 1, j - 1)) : 0;

            return Math.max(p1, Math.max(p2, p3));
        }
    }

    // ========================================
    // 动态规划

    public static int process2(String s1, String s2){
        if(s1 == null || s2 == null || s1.length() == 0 || s2.length() == 0){
            return 0;
        }
        char[] str1 = s1.toCharArray();
        char[] str2 = s2.toCharArray();

        int N = s1.length();
        int M = s2.length();
        int[][] dp = new int[N][M];

        dp[0][0] = str1[0] == str2[0] ? 1 : 0;
        // i == 0, 填充第一行
        for(int row = 1; row < M; row++){
            dp[0][row] = str1[0] == str2[row] ? 1 : dp[0][row - 1];
        }
        // j == 0, 填充第一列
        for(int col = 1; col < N; col++){
            dp[col][0] = str1[col] == str2[0] ? 1 : dp[col - 1][0];
        }
        // 一般位置

        for(int col = 1; col < N; col++){
            for(int row = 1; row < M; row++){
                int p1 = dp[col][row - 1];
                int p2 = dp[col - 1][row];
                int p3 = str1[col] == str2[row] ? (1 + dp[col - 1][row - 1]) : 0;

                dp[col][row] = Math.max(p1, Math.max(p2, p3));
            }
        }
        return dp[N - 1][M - 1];
    }


    public static void main(String[] args) {

    }

}
