package com.dreams.algorithm.dynamic;

/**
 * 规定1和A对应，2和B对应，3和C对应.....
 * 那么一个数字字符串比如"111" 就可以转化为："AAA"， "KA"， "AK"
 * 给定一个只有数字字符组成的字符串str，返回有多少种转化结果
 */
public class StringConversion {


    public static int number(String str){
        if(str == null || str.length() < 1){
            return 0;
        }
        char[] chars = str.toCharArray();
        return process(chars, 0);
    }

    /**
     *
     * @param str
     * @param index
     * @return
     */
    public static int process(char[] str, int index){
        // [0, index - 1]上已经转换完成，无需过问，此时表示有一种方法
        if(index == str.length){
            return 1;
        }

        // index没有到最后， 说明有字符
        if(str[index] == '0'){ // 说明之前的决策有问题
            return 0;
        }

        // str[index] != '0'

        // 可能性一，index单转
        int ways = process(str, index + 1);
        if(index + 1 < str.length && (str[index] - '0') * 10 + (str[index + 1] - '0') < 27){
            ways += process(str, index + 2);
        }

        return ways;
    }


    //==================================
    // 动态规划

    public static int process2(String str){
        if(str == null || str.length() < 1){
            return 0;
        }
        char[] chars = str.toCharArray();
        int N = chars.length;

        int[] dp = new int[N + 1];
        dp[N] = 1;
        for(int index = N - 1; index >= 0; index--){

            if(chars[index] != 0){
                int ways = dp[index + 1];
                if(index + 1 < chars.length && (chars[index] - '0') * 10 + (chars[index + 1] - '0') < 27){
                    ways += dp[index + 2];
                }
                dp[index] = ways;
            }
        }
        return dp[0];
    }









    public static void main(String[] args) {
        System.out.println("ans = " + number("111"));
        System.out.println("ans = " + process2("111"));
    }


}
