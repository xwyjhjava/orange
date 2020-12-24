package com.dreams.algorithm.leetcode.simple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/12/23 20:06
 * @description TODO
 */
public class Problem_387_FirshUniqueCharacter {

	public static void main(String[] args) {

	}


	/**
	 * 解法是O(N)， 但是空间复杂度过高
	 * @param s
	 * @return
	 */
	public int firstUniqChar(String s) {

		if(s.length() < 1){
			return -1;
		}
		Map<Character, Integer> helpMap = new HashMap<>();
		List<Character> black = new ArrayList<>();
		for(int i = 0; i < s.length(); i++){
			char cur = s.charAt(i);
			if(!helpMap.containsKey(cur) && !black.contains(cur)){
				helpMap.put(cur, i);
			}else{
				helpMap.remove(cur);
				black.add(cur);
			}
		}

		if(helpMap.size() < 1){
			return -1;
		}
		// 遍历map, 返回value最小的那个值
		int ans = Integer.MAX_VALUE;
		for(Integer ele: helpMap.values()){
			ans = Math.min(ele, ans);
		}
		return ans;
	}



	public int firstUniqChar2(String s){
		if(s.length() < 1){
			return -1;
		}

		Map<Character, Integer> helpMap = new HashMap<>();
		for(int i = 0; i < s.length(); i++){
			char cur = s.charAt(i);
			helpMap.put(cur, helpMap.getOrDefault(cur, 0) + 1);
		}

		for(int i = 0; i < s.length(); i++){
			if(helpMap.get(s.charAt(i)) == 1){
				return i;
			}
		}
		return -1;
	}




}
