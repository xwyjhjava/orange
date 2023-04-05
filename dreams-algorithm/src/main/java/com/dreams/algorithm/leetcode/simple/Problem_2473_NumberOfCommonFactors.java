package com.dreams.algorithm.leetcode.simple;

/**
 * 给你两个正整数 a 和 b ，返回 a 和 b 的 公因子的数目。
 *
 * 如果 x 可以同时整除 a 和 b ，则认为 x 是 a 和 b 的一个 公因子 。
 */
public class Problem_2473_NumberOfCommonFactors extends Solution{

    @Override
    public void solute() {
        int a = 6;
        int b = 3;
        commonFactors(a, b);
    }

    // 方法一
    public int commonFactors(int a, int b) {
        int small = Math.min(a, b);
        if (small == 1) {
            return 1;
        }
        int big = Math.max(a, b);
        int threadbare = small >> 1;
        int result = 1;
        for (int i = 2; i <= threadbare; i++) {
            if (a % i == 0 && b % i == 0) {
                result++;
            }
        }
        // 再判断一下小的本身的是不是大的因子
        if (big % small == 0) {
            result++;
        }
        return result;
    }

    // 方法二 ： 枚举到最大公因数
    public int commonFactorsFunc2(int a, int b) {
        int c = gcb(a, b);
        int ans = 0;
        for (int i = 0; i * i <= c; i++) {
            if (c % i == 0) {
                ans++;
                if (i * i != c) {
                    ans++;
                }
            }
        }
        return ans;
    }

    // 方法三 ： 枚举到最小值
    public int commonFactorsFunc3(int a, int b) {
        int ans = 0;
        for (int x = 1; x <= Math.min(a, b); ++x) {
            if (a % x == 0 && b % x == 0) {
                ++ans;
            }
        }
        return ans;
    }

    // 求最大公约数
    public static int gcb(int a, int b) {
        while (b != 0) {
            a %= b;
            a ^= b;
            b ^= a;
            a ^= b;
        }
        return a;
    }

}

