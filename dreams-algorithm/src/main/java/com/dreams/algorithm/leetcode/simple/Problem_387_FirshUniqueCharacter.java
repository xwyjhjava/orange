package com.dreams.algorithm.leetcode.simple;

import java.util.HashMap;
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


	public int firstUniqChar(String s) {

		if(s.length() < 1){
			return -1;
		}
		Map<Character, Integer> helpMap = new HashMap<>();

		char first = s.charAt(0);
		for(int i = 0; i < s.length(); i++){
			char cur = s.charAt(i);
			if(!helpMap.containsKey(cur)){
				helpMap.put(cur, i);
			}else{
				helpMap.remove(cur);
			}
		}
		return 0;
	}


}
