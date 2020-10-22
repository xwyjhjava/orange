package com.dreams.algorithm.leetcode.medium;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/10/22 11:09
 * @description TODO
 */
public class Problem_763_PartitionLabels {


	public static void main(String[] args) {
		String S = "ababcbacadefegdehijhklij";

	}


	/**
	 * A string S of lowercase English letters is given.
	 * We want to partition this string into as many parts as possible so that each letter appears in at most one part,
	 * and return a list of integers representing the size of these parts.
	 *
	 *
	 * 解法：
	 *      1. 贪心算法
	 *      2. 记录每个字符串中每个字母出现的最后位置end
	 *      3. 遍历字符串， 得到当前字母出现的最后位置end'， 则当前片段满足end=max(end, end')
	 *      4. 当index==end时， 当前片段访问结束[start, end],  长度为end - start + 1, 更新start=end+1
	 *
	 * @param S  1<= S.length <= 500, contains of lowercase English letters('a' to 'z') only
	 * @return
	 */
	public List<Integer> partitionLabels(String S){
		// 记录每个字母出现的最后位置下标
		int[] last = new int[26];
		int length = S.length();
		int start = 0;
		int end = 0;

		List<Integer> ans = new ArrayList<>();
		// 初始化last数组
		for (int i = 0; i < length; i++) {
			last[S.charAt(i) - 'a'] = i;
		}
		for (int index = 0; index < length; index++) {
			// 更新当前字母的出现的最后位置的max值
			end = Math.max(last[S.charAt(index) - 'a'], end);
			// 当 i 来到 end时， 此时一个片段即可算出
			// index来到end的位置
			if(index == end){
				ans.add(end - start + 1);
				// start 来到end的后一个位置
				start = end + 1;
			}
		}
		return ans;
	}

}
