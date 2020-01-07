package com.dreams.algorithm.leetcode.simple;

import java.util.ArrayList;
import java.util.List;

/**
 * 给出一个 32 位的有符号整数，你需要将这个整数中每位上的数字进行反转。
 * 假设我们的环境只能存储得下 32 位的有符号整数，则其数值范围为 [−231,  231 − 1]。
 * 请根据这个假设，如果反转后整数溢出那么就返回 0。
 */
public class 整数反转 extends Solution{

    @Override
    void solute() {
        super.solute();

        int x = 56;
        int result = high_reverse(x);
        System.out.println(result);
    }

    /**
     *
     * @param x
     * @return
     *
     * 思路：x转成字节数组
     */
    public int reverse(int x) {

       //处理溢出
        if(x > 2147483647 || x <= -2147483648){
            return 0;
        }
        //int 转char数组
        String xStr = Integer.toString(Math.abs(x));
        char[] chars = xStr.toCharArray();

        int length = chars.length;
        if(length % 2 == 0){
            //偶数
            for (int i = 0; i < length / 2; i++) {
                char temp = chars[i];
                chars[i] = chars[length - 1 - i];
                chars[length - 1 - i] = temp;
            }
        }else {
            //奇数
            for (int i = 0; i < (length - 1) / 2; i++) {
                char temp = chars[i];
                chars[i] = chars[length - 1 - i];
                chars[length - 1 - i] = temp;
            }
        }
        StringBuilder builder = new StringBuilder();
        if(x < 0){
            builder.append("-");
        }
        for (char c : chars) {
            builder.append(c);
        }
        //如何判断溢出异常
        long result = Long.parseLong(builder.toString());
        if(result > 2147483647 || result < -2147483648){
            return 0;
        }else{
            return (int)result;
        }
    }

    // TODO: 2019/11/29  改进一：倒序遍历； 改进二： 数学算法

    public int high_reverse(int i){

        return Integer.reverse(i);

    }
}
