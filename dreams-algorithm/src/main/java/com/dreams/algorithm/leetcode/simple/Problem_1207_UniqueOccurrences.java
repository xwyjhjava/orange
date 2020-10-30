package com.dreams.algorithm.leetcode.simple;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/10/28 14:56
 * @description TODO
 */
public class Problem_1207_UniqueOccurrences {

	public static void main(String[] args) {
		String abc = "asda";
		char[] str = abc.toCharArray();
		String ans = str[0]+ "";
	}

	/**
	 * Given an array of Integers arr, write a function than returns true if
	 * and only if the number of occurrences of each value in the array is unique.
	 *
	 * 解法： HashMap
	 * @param arr
	 * @return
	 */
	public boolean uniqueOccurrences(int[] arr) {
		if(arr.length < 2){
			return true;
		}
		Map<Integer, Integer> map = new HashMap<>();
		// 每次number出现的次数
		for(int i = 0; i < arr.length; i++){
			map.put(arr[i], map.getOrDefault(arr[i], 0) + 1);
		}
		// 辅助map
		Map<Integer, Integer> helpMap = new HashMap<>();
		for(Map.Entry<Integer, Integer> entry: map.entrySet()){
			int key = entry.getKey();
			int value = entry.getValue();

			if(helpMap.containsKey(value)){
				return false;
			}else{
				helpMap.put(value, key);
			}
		}

		return true;
	}




	public boolean uniqueOccurrences2(int[] arr) {
		if(arr.length < 2){
			return true;
		}
		Map<Integer, Integer> map = new HashMap<>();
		// 每次number出现的次数
		for(int i = 0; i < arr.length; i++){
			map.put(arr[i], map.getOrDefault(arr[i], 0) + 1);
		}
		// 用HashSet, 利用set的去重特性
		Set<Integer> helpSet = new HashSet<>();
		for(Map.Entry<Integer, Integer> entry: map.entrySet()){
			int value = entry.getValue();
			helpSet.add(value);
		}
		return helpSet.size() == map.size();
	}

}
