package com.dreams.algorithm.leetcode.simple;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/10/9 10:14
 * @description TODO
 */
public class Problem_344_ReverseString {

	public static void main(String[] args) {
		String str = "hello";
		reverseString(str.toCharArray());
	}

	public static void reverseString(char[] s){
		int maxIndex = s.length - 1;
		for (int i = 0; i < s.length / 2; i++) {
			char tmp = s[i];
			s[i] = s[maxIndex - i];
			s[maxIndex - i] = tmp;
		}
		for (int i = 0; i < s.length; i++) {
			System.out.print(" " + s[i]);
		}
	}
}
