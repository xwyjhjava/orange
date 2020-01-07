package com.dreams.algorithm.leetcode.simple;

import java.util.ArrayList;
import java.util.List;

/**
 * 判断一个整数是否是回文数。回文数是指正序（从左向右）和倒序（从右向左）读都是一样的整数。
 */
public class 回文数 extends Solution{

    @Override
    void solute() {
        super.solute();
        isPalindrome_2(121);
    }

    //正序遍历和倒序遍历可解
    //进阶：不将整数转换成字符串进行处理
    public boolean isPalindrome(int x){

        int temp = Math.abs(x);
        if(temp < 10){
            return true;
        }
        List<Integer> palindromeList = new ArrayList<>();
        while(true) {
            int a = temp / 10;
            int b = temp % 10;
            if (temp < 10) {
               palindromeList.add(temp);
               break;
            }else{
                palindromeList.add(b);
                temp = a;
            }
        }
        palindromeList.stream().forEach(ele -> System.out.println(ele));
        //TODO 对集合进行先序和后序遍历即可解
        return false;

    }

    /**
     * 按位分解，重新组合可得一个整数，和原数比较
     * @return
     */
    public boolean isPalindrome_2(int x){
        //负数一定不是回文序列
        if(x < 0){
            return false;
        }
        int right;
        int left = x;
        int temp = 0;
        while(left != 0) {
            //取模, 121 -> 1
            right = left % 10;
            //取余, 121 -> 12
            left = left / 10;
            //重新拼凑, 0 * 10 + 1
            temp = temp * 10 + right;
        }
        return temp == x;
    }

}
