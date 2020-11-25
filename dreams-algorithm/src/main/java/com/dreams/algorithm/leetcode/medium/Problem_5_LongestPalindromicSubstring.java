package com.dreams.algorithm.leetcode.medium;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/10/28 16:09
 * @description TODO
 */
public class Problem_5_LongestPalindromicSubstring {

	public static void main(String[] args) {
		String s = "aacabdkacaa";
		String ans = longestPalindrome_test(s);
		System.out.println("ans = " + ans);
	}


	/**
	 * Given a string s, return the longest palindromic substring in s.
	 *
	 * 解法： Manacher 算法
	 * @param s 1 <= s.length <= 1000
	 *          s consist of only digits and English letters (lower-case and/or upper-case)
	 * @return
	 */
	public static String longestPalindrome(String s){
		if(s.length() == 1){
			return s;
		}





		return s;
	}

	// 回文串的判断
	public static boolean isPalindrome(String str){
		String reverse = new StringBuilder(str).reverse().toString();
		return reverse == str;
	}


	/**
	 * 尝试
	 * @param s
	 * @return
	 */
	public static String longestPalindrome_test(String s) {
		// 1 <= s.length <= 1000
		if(s.length() == 1){
			return s;
		}
		// s.length >= 2
		int start = 0;
		int end = 0;

		char[] str = s.toCharArray();
		// ans 是默认回文串是字符串的第一个字符
		String ans = str[0] + "";
		Map<Character, Integer> lastMap = new HashMap<>();

		// 遍历一遍， 找到字符出现的最后位置
		for(int i = 0; i < str.length; i++){
			lastMap.put(str[i], i);
		}

		for(int i = 0; i < str.length; i++){
			//int curIndex = str[i];
			int lastIndex = lastMap.get(str[i]);
			// 判断 i ~~ lastIndex 是否是回文
			if(i == lastIndex){
				continue;
			}
			start = i;
			end = lastIndex;
			// 回文判断
			while(i < lastIndex){
				if(str[i + 1] == str[lastIndex - 1]){
					i = i + 1;
					lastIndex = lastIndex - 1;
				}else{ // 字符串不是回文
					start = lastIndex;
					break;
				}
			}
			// 当前的回文串比历史串长度大
			if(end - start > ans.length() - 1){
				ans = "";
				for(int index = start; index <= end; index++){
					ans = ans + str[index];
				}
			}
		}
		return ans;
	}
}
