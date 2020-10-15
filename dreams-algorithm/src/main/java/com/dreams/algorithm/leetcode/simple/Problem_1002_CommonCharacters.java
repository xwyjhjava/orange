package com.dreams.algorithm.leetcode.simple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/10/14 11:45
 * @description TODO
 */
public class Problem_1002_CommonCharacters {

	public static void main(String[] args) {
		String[] A = {"label", "bella", "roller"};
		List<String> result = commonChars(A);
		for(String ele : result){
			System.out.print("  " + ele);
		}
	}

	public static List<String> commonChars(String[] A){
		if(A == null || A.length < 1){
			return null;
		}
		List<String> result = new ArrayList<>();
		// 字符计数
		Map<Character, Integer> charOldMap = new HashMap<>();
		// 取第一个数
		char[] firstCharArray = A[0].toCharArray();
		for(int i = 0; i < firstCharArray.length; i++){
			charOldMap.put(firstCharArray[i], charOldMap.getOrDefault(firstCharArray[i], 0) + 1);
		}
		// 第一个字符组成map
		for (int index = 1; index < A.length; index++) {
			Map<Character, Integer> charNewMap = new HashMap<>();
			for (int i = 0; i < A[index].length(); i++) {
				String tmp = A[index];
				// oldmap 中能找到, 则添加到新Map中，否则不添加
				if(charOldMap.containsKey(tmp.charAt(i))){
					charNewMap.put(tmp.charAt(i), Math.min(charNewMap.getOrDefault(tmp.charAt(i), 0) + 1, charOldMap.get(tmp.charAt(i))));
				}
			}
			// newMap 更新成oldMap
			charOldMap = charNewMap;
		}
		// 遍历charOldMap， 得到结果
		for(Character c : charOldMap.keySet()){
			Integer nums = charOldMap.get(c);
			for (int i = 0; i < nums; i++) {
				result.add(String.valueOf(c));
			}
		}
		return result;
	}


}
