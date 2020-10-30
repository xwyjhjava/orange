package com.dreams.algorithm.leetcode.simple;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/10/29 14:12
 * @description TODO
 */
public class Problem_14_LongestCommonPrefix {

	public static void main(String[] args) {
		String[] strs = {"","b"};
		String prefix = longestCommonPrefix(strs);
		System.out.println("prefix = " + prefix);
	}

	/**
	 * Write a function to find the longest common prefix string
	 * amongst an array of strings.
	 * if there is no common prefix, return an empty string "".
	 *
	 * @param strs  0 <= str.length <= 200
	 *              0 <= str[i].length <= 200
	 *              str[i] consist of only lower-case English letters
	 * @return
	 */
	public static String longestCommonPrefix(String[] strs) {
		if(strs.length < 1 || strs[0] == null){
			return "";
		}
		// 如果存在最长子串， 则一定在第一个字符中
		String pattern = strs[0];
		String tmp = "";
		for (int i = 1; i < strs.length; i++) {
			// 如果此时pattern=="", 则说明此字符数组不存在公共子串
			if(strs[i] == "" || pattern == ""){
				return "";
			}

			if(strs[i] != "") {
				int index = 0;
				while (index < strs[i].length()) {
					if (pattern.charAt(index) == strs[i].charAt(index)) {
						tmp = tmp + pattern.charAt(index);
						index++;
						continue;
					}
					break;
				}
			}else{
				return "";
			}
			// 更新模式串
			pattern = tmp;
			// 清空临时串
			tmp = "";
		}
		return pattern;
	}

}
