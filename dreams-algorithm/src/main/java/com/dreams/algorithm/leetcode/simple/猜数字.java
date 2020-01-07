package com.dreams.algorithm.leetcode.simple;

/**
 *
 * 小A 和 小B 在玩猜数字。小B 每次从 1, 2, 3 中随机选择一个，小A 每次也从 1, 2, 3 中选择一个猜。他们一共进行三次这个游戏，
 * 请返回 小A 猜对了几次？
 *
 * 输入的guess数组为 小A 每次的猜测，answer数组为 小B 每次的选择。guess和answer的长度都等于3。
 */
public class 猜数字 extends Solution{

    @Override
    void solute() {
        super.solute();
        System.out.println("网址：https://leetcode-cn.com/problems/guess-numbers/");

        int[] guess = {1, 2, 3};
        int[] answer = {1, 2, 1};

        int result = game(guess, answer);
        System.out.println(result);
    }

    /**
     *
     * @param guess
     * @param answer
     * @return 猜对的次数
     *
     * 比较下标即可，单循环，次数有限的情况下，也可以多个if判断
     */
    public int game(int[] guess, int[] answer){
        int flag = 0;
        for (int i = 0; i < guess.length; i++) {
            if(guess[i] == answer[i]){
                flag ++;
            }
        }
        return flag;
    }
}
