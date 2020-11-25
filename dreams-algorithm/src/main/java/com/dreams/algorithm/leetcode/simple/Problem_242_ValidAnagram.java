package com.dreams.algorithm.leetcode.simple;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/11/22 23:02
 * @description TODO
 */
public class Problem_242_ValidAnagram {

	public static void main(String[] args) {

	}

	/**
	 * Given tow strings sand t, write a function to determine if t is an anagram
	 * of s.
	 *
	 * you may assume the string contains only lowercase alphabets.
	 *
	 * 解法： HashMap
	 * @param s
	 * @param t
	 * @return
	 */
	public static boolean isAnagram(String s, String t){

		if(s == "" || t == ""){
			return false;
		}

		// 字母的数量一致
		// 利用可以假设字符串只包含小写字母的特性
		int[] help = new int[26];

		char[] sArray = s.toCharArray();
		char[] tArray = t.toCharArray();

		// 计算字符串s中字符的个数
		for(int i = 0; i < sArray.length; i++){
			help[sArray[i] - 'a']++;
		}

		for(int j = 0; j < tArray.length; j++){
			help[tArray[j] - 'a']--;
		}

		// 如果help中不全都为0, 则说明t不是s的字母异位词
		for(int ele : help){
			if(ele != 0){
				return false;
			}
		}

		return true;
	}


}
