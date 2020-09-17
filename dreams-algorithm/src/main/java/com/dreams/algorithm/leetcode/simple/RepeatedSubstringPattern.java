package com.dreams.algorithm.leetcode.simple;

import java.util.Arrays;

/**
 * 重复子串问题
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/9/4 10:40
 * @description TODO
 */
public class RepeatedSubstringPattern extends Solution{


	@Override
	void solute() {
		super.solute();
//		ABABAAABABAA
		boolean flag = getNextList("aba");
		System.out.println("flag = " + flag);
		System.out.println("==========================");
//		kmp("abacabacf");
		System.out.println("==========================");
//		getNexts("abacabacf");

		System.out.println("==========================");

//		findSubStr("abcdefabace", "abac");
	}


	/**
	 * 预处理得到模式串的next数组
	 * @param pattern
	 */
	public boolean getNextList(String pattern){
		// 模式串的长度
		int m = pattern.length();
		int[] next = new int[m];
		// 标记前缀的的起始位置， 也可拿来表示模式串中重复子串的长度
		int key = 0;
		// 从第二个位置开始
		for (int i = 2; i < m; i++) {

			// 比较 pattern 的 key位置的字符和 i - 1位置的字符
			// 当 key位置的字符和 i-1位置的字符不相同时, 指针回溯到上一个位置
			while (key != 0 && pattern.charAt(key) != pattern.charAt(i - 1)){
				key = next[key];
			}
			// 字符相同时， 更新next
			if(pattern.charAt(key) == pattern.charAt(i - 1)){
				// 更新重复子串长度
				next[i] = key + 1;
				key = key + 1;
			}
		}

		for (int i = next.length - 1; i >= 0; i--) {
			System.out.println("i_" + i  + " = "+ next[i]);
		}

		// 满足条件的特性
//		return next[m - 1] != 0 && m % (m - next[m -1] -1) == 0;

		String query = pattern + pattern;
		int n = query.length();

		int match = 0;
		// 循环主串
		for (int i = 1; i < n - 1; i++) {
			while(match > 0 && query.charAt(i) != pattern.charAt(match)){
				match = next[match];
			}
			if(query.charAt(i) == pattern.charAt(match)){
				match++;
			}
			if(match == m){
				return true;
			}
		}
		return false;
	}

	/**
	 *  leetcode官方解法
	 * @param pattern
	 * @return
	 */
	public boolean kmp(String pattern) {

		int n = pattern.length();
		int[] fail = new int[n];
		Arrays.fill(fail, -1);
		for (int i = 1; i < n; ++i) {
			int j = fail[i - 1];
			while (j != -1 && pattern.charAt(j + 1) != pattern.charAt(i)) {
				j = fail[j];
			}
			if (pattern.charAt(j + 1) == pattern.charAt(i)) {
				fail[i] = j + 1;
			}
		}

		for (int i = fail.length - 1; i >= 0; i--) {
			System.out.println("i_" + i  + " = "+ fail[i]);
		}
		return fail[n - 1] != -1 && n % (n - fail[n - 1] - 1) == 0;
	}

	/**
	 * 小灰解法
	 * @param pattern
	 * @return
	 */
	public static int[] getNexts(String pattern) {
		int[] next = new int[pattern.length()];
		int j = 0;
		for (int i=2; i<pattern.length(); i++) {
			while (j != 0 && pattern.charAt(j) != pattern.charAt(i-1)) {
				//从next[i+1]的求解回溯到 next[j]
				j = next[j];
			}
			if (pattern.charAt(j) == pattern.charAt(i-1)) {
				j++;
			}
			next[i] = j;
		}
		return next;
	}


	/**
	 * 查找子串
 	 */
	public void findSubStr(String str, String pattern){
		int[] nexts = getNexts(pattern);
		int j = 0 ;

		for (int i = 0; i < str.length(); i++) {
			// 当模式串和主串节点字符不匹配时， 查询next数组，找到下一个起点
			while (j > 0 && str.charAt(i) != pattern.charAt(j)){
				j = nexts[j];
			}
			// 匹配的话， 则j++
			if(str.charAt(i) == pattern.charAt(j)){
				j++;
			}

			// 匹配结束
			if(j == pattern.length()){
				int result = i - pattern.length() + 1;
				System.out.println("==============================");
				System.out.println("result = " + result);
			}

		}



	}

}
