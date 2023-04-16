package com.dreams.algorithm.leetcode;

public class LeetCodeWeekCompetition {

    public static void main(String[] args) {

//        int[] nums = new int[]{73, 13, 20, 6};
//        int[] divisors = new int[]{56, 75, 83, 26, 24, 53, 56, 61};
//        int ans = maxDivScore(nums, divisors);
//        System.out.println("ans = " + ans);
        String word = "abc";
        addMinimum(word);
    }

    public static int maxDivScore(int[] nums, int[] divisors) {

        int index = 0;
        int max = 0;
        int minVal = divisors[0];
        for (int i = 0; i < divisors.length; i++) {
            int count = 0;
            for (int j = 0; j < nums.length; j++) {
                if (nums[j] % divisors[i] == 0) {
                    count++;
                }
            }
            if (count > max) { // 得分最大
                System.out.println("count = " + count);
                max = count;
                minVal = divisors[i];
                index = i;
            } else if (count == max) {
                if (minVal > divisors[i]) {
                    minVal = divisors[i];
                    index = i;
                }
            }
        }
        return divisors[index];
    }


    public static int addMinimum2(String word) {
        int count = 0, i = 0;
        for (int j = 0; j < word.length(); i++) {
            if (word.charAt(j) == i % 3 + 'a') {
                j++;
            } else {
                count++;
            }
        }
        return count + (3 - i % 3) % 3;
    }


    public static int addMinimum(String word) {

        int count = 0;
        int step = 0;
        String target = "abc";
        for (int i = 0; i < word.length(); step++) {
            if (word.charAt(i) == target.charAt(step % 3)) {
                i++;
            } else {
                count++;
            }
        }
        return step % 3 == 0 ? count : count + (3 - step % 3);
    }

}
