package com.dreams.algorithm.dynamic;

import java.util.HashMap;
import java.util.Map;

/**
 * 贴纸问题
 *
 * 给定一个字符串 str, 给定一个字符串类型的数组arr, 出现的字符都是小写英文
 * arr的每一个字符串，代表一张贴纸，你可以把单个字符剪开使用，目的是拼出str来
 * 返回需要至少多少张贴纸可以完成这个任务
 *
 * 例： str="babac"， arr={"ba","c","abcd"}
 *
 * Leetcode 原题 ： 691 stickers-to-spell word
 *
 */
public class Sticker {


    public static int minSticker1(String[] stickers, String target){
        return process(stickers, target);
    }

    /**
     *
     * @param stickers 所有贴纸，每一张贴纸都有无穷张
     * @param target 目标串
     * @return 最少张数
     */
    public static int process(String[] stickers, String target){
        // 如果target没有了，那么还需要0张
        if(target.length() == 0){
            return 0;
        }
        int ans = Integer.MAX_VALUE;
        // target还有, 则按贴纸按个尝试
        for (String first: stickers) {
            // 用了first之后，target还剩什么
            String rest = minusStr(first, target);
            // 如果rest.length == target.length，表示第一张贴纸怎么都搞不定
            if(rest.length() != target.length()){
                int next = process(stickers, rest);
                ans = Math.min(ans, next);
            }
        }
        return ans + (ans == Integer.MAX_VALUE ? 0 : 1);
    }

    public static String minusStr(String first, String target){
        char[] charsFirst = first.toCharArray();
        char[] charsTarget = target.toCharArray();

        // 计数
        int[] count = new int[26];
        for (int i = 0; i < charsTarget.length; i++) {
            count[charsTarget[i] - 'a']++;
        }

        for (int i = 0; i < charsFirst.length; i++) {
            count[charsFirst[i] - 'a']--;
        }
        StringBuilder stringBuilder = new StringBuilder();
        // 剩余的字符拼接出来
        for(int i = 0; i < count.length; i++){
            if(count[i] > 0){
                for(int j = 0; j < count[i]; j++){
                    stringBuilder.append((char)(i + 'a'));
                }
            }
        }
        return stringBuilder.toString();
    }


    //============================================
    // 剪枝优化
    public static int minSticker2(String[] stickers, String target){

        int N = stickers.length;
        // stickers 词频化
        int[][] dp = new int[N][26];
        for (int i = 0; i < N; i++) {
            for (char cha : stickers[i].toCharArray()) {
                dp[i][cha - 'a']++;
            }
        }
        int ans = process2(dp, target);
        return ans == Integer.MAX_VALUE ? -1 : ans;
    }

    /**
     *
     * @param stickers 字符贴纸的词频统计
     * @param target
     * @return
     */
    public static int process2(int[][] stickers, String target){

        if(target.length() == 0){
            return 0;
        }
        // target 做出词频统计
        int[] countTarget = new int[26];
        char[] charsTarget = target.toCharArray();
        for (char cha : charsTarget) {
            countTarget[cha - 'a'] ++;
        }
        int ans = Integer.MAX_VALUE;
        int N = stickers.length;
        for(int i = 0; i < N; i++){
            // 表示第一张尝试的贴纸，存在target的第一位字符
            /** 剪枝优化 */
            if(stickers[i][charsTarget[0] - 'a'] > 0){
                // target - 第一张贴纸
                StringBuilder rest = new StringBuilder();
                for(int j = 0; j < 26; j++){
                    if(stickers[i][j] > 0){
                        int nums = countTarget[j] - stickers[i][j];
                        if(nums > 0){
                            for(int k = 0; k < nums; k++){
                                rest.append((char)(j + 'a'));
                            }
                        }
                    }
                }
                int next = process2(stickers, rest.toString());
                ans = Math.min(ans, next);
            }

        }
        return ans + (ans == Integer.MAX_VALUE ? 0 : 1);
    }


    //====================================
    // 缓存优化

    public static int minSticker3(String[] stickers, String target){

        int N = stickers.length;
        // stickers 词频化
        int[][] dp = new int[N][26];
        for (int i = 0; i < N; i++) {
            for (char cha : stickers[i].toCharArray()) {
                dp[i][cha - 'a']++;
            }
        }
        Map<String, Integer> hashMap = new HashMap<>();
        hashMap.put("", 0);
        int ans = process3(dp, target, hashMap);
        return ans == Integer.MAX_VALUE ? -1 : ans;
    }

    /**
     *
     * @param stickers 字符贴纸的词频统计
     * @param target
     * @return
     */
    public static int process3(int[][] stickers, String target, Map<String, Integer> hashMap){
        if(hashMap.containsKey(target)){
            return hashMap.get(target);
        }

        if(target.length() == 0){
            return 0;
        }
        // target 做出词频统计
        int[] countTarget = new int[26];
        char[] charsTarget = target.toCharArray();
        for (char cha : charsTarget) {
            countTarget[cha - 'a'] ++;
        }
        int ans = Integer.MAX_VALUE;
        int N = stickers.length;
        for(int i = 0; i < N; i++){
            // 表示第一张尝试的贴纸，存在target的第一位字符
            /** 剪枝优化 */
            if(stickers[i][charsTarget[0] - 'a'] > 0){
                // target - 第一张贴纸
                StringBuilder rest = new StringBuilder();
                for(int j = 0; j < 26; j++){
                    if(stickers[i][j] > 0){
                        int nums = countTarget[j] - stickers[i][j];
                        if(nums > 0){
                            for(int k = 0; k < nums; k++){
                                rest.append((char)(j + 'a'));
                            }
                        }
                    }
                }
                int next = process3(stickers, rest.toString(), hashMap);
                ans = Math.min(ans, next);
            }

        }
        hashMap.put(target, ans + (ans == Integer.MAX_VALUE ? 0 : 1));
        return ans + (ans == Integer.MAX_VALUE ? 0 : 1);
    }





    public static void main(String[] args) {

        String[] stickers = {"ba","c","abcd"};
        String target = "babac";

        System.out.println("ans == " + minSticker1(stickers, target));
        System.out.println("ans == " + minSticker2(stickers, target));
        System.out.println("ans == " + minSticker3(stickers, target));
    }
}
