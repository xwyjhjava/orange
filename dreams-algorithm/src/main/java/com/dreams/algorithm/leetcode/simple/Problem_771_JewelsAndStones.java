package com.dreams.algorithm.leetcode.simple;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/10/2 22:33
 * @description TODO
 */
public class Problem_771_JewelsAndStones {

	public static void main(String[] args) {
		String J = "aA";
		String S = "aAAbbb";
		int result = numJewelsInStones(J, S);
		System.out.println("result = " + result);
	}

	/**
	 * 字符串 J 中的字符在字符 S 中出现的次数
	 * @param J
	 * @param S
	 * @return
	 */
	public static int numJewelsInStones(String J, String S) {
		if(J == null || J.isEmpty()){
			return 0;
		}
		if(S == null || S.isEmpty()){
			return 0;
		}
		Map<Character, Integer> jmap = new HashMap<>();
		char[] jchars = J.toCharArray();
		// J put到map
		for (int i = 0; i < jchars.length; i++) {
			jmap.put(jchars[i], 0);
		}
		char[] schars = S.toCharArray();
		for(char c: schars){
			Integer count = jmap.get(c);
			// 在 S 中能找到 J中的字符
			if(count != null){
				// 更新count
				jmap.put(c, count + 1);
			}
		}
		// jmap的字符集相加
		Integer result = 0;
		for (char jchar: jchars){
			result = result + jmap.get(jchar);
		}
		return result;
	}
}
