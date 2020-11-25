package com.dreams.algorithm.leetcode.simple;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/11/25 10:11
 * @description TODO
 */
public class Problem_1370_IncreasingDecreasingString {

	public static void main(String[] args) {
		String s = "spo";
		String ans = sortString(s);
		System.out.println("ans = " + ans);
	}


	/**
	 * Given a string s. You should re-order the string using the following algorithm:
	 *
	 * 1. Pick the smallest character from s and append it to the result.
	 * 2. Pick the smallest character from s which is greater than the last appended character to the result and append it.
	 * 3. Repeat step 2 until you cannot pick more characters.
	 * 4. Pick the largest character from s and append it to the result.
	 * 5. Pick the largest character from s which is smaller than the last appended character to the result and append it.
	 * 6. Repeat step 5 until you cannot pick more characters.
	 * 7. Repeat the steps from 1 to 6 until you pick all characters from s.
	 *
	 * In each step, If the smallest or the largest character appears more than once you can choose any occurrence and
	 * append it to the result.
	 *
	 * Return the result string after sorting s with this algorithm.
	 *
	 * 来源：力扣（LeetCode）
	 * 链接：https://leetcode-cn.com/problems/increasing-decreasing-string
	 * 著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
	 *
	 *  @param s  1 <= s.length <= 500
	 *            s contains only lower-case English letter
	 * @return
	 */
	public static String sortString(String s){
		if(s.length() < 2){
			return s;
		}
		char[] array = s.toCharArray();
		StringBuilder ans = new StringBuilder();
		// 辅助数组, 记录 s串中字符按a-z的顺序出现的频次
		// 申请的数组可以进一步压缩, 只要先找出s串的最大字符
		int[] help = new int[26];
		for(int i = 0; i < array.length; i++){
			help[array[i] - 'a']++;
		}
		while(true) {
			// 1,2,3步
			for (int i = 0; i < help.length; i++) {
				if (help[i] > 0) {
					ans.append((char) (i + 'a'));
					help[i]--;
				}
			}
			if(ans.length() == array.length){
				break;
			}
			//4,5,6步
			for (int i = help.length - 1; i >= 0; i--) {
				if (help[i] > 0) {
					ans.append((char)(i + 'a'));
					help[i]--;
				}
			}
			if(ans.length() == array.length){
				break;
			}
		}
		return ans.toString();
	}



}
