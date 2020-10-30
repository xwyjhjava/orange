package com.dreams.algorithm.leetcode.simple;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/10/29 11:36
 * @description TODO
 */
public class Problem_13_RomanToInteger {

	public static void main(String[] args) {
		String s = "MCMXCIV";
		int sum = romanToInt(s);
		System.out.println("sum = " + sum);
	}


	public static int romanToInt(String s){

		Map<Character, Integer> charMap = new HashMap<Character, Integer>(){
			{
				put('I', 1);
				put('V', 5);
				put('X', 10);
				put('L', 50);
				put('C', 100);
				put('D', 500);
				put('M', 1000);
			}
		};

		int sum = 0;
		char[] str = s.toCharArray();
		// 第一个值
		int preNum = charMap.get(str[0]);
		// 循环从 i == 1的位置开始
		for (int i = 1; i < str.length; i++) {
			Integer cur = charMap.get(str[i]);
			// 当前值比前面一个小, 则加
			// 否则就减
			if(preNum >= cur){
				sum = sum + preNum;
			}else{
				sum = sum - preNum;
			}
			// 更新 preNum
			preNum = cur;
		}
		sum = sum + preNum;
		return sum;
	}

}
