package com.dreams.algorithm.leetcode.hard;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.hard
 * @date 2020/11/11 10:40
 * @description TODO
 */
public class Problem_514_FreedomTrail {

	public static void main(String[] args) {

		String ring = "godding";
		String key = "gd";
		int ans = findRotateSteps2(ring, key);
//		int rSize = 4;
//		int tmp = dial(1, 2, rSize) + 1;
//		System.out.println("tmp = " + tmp);
	}

	/**
	 * i2来到i1位置的最少步数函数
	 * @param i1
	 * @param i2
	 * @param size
	 * @return
	 */
	public static int dial(int i1, int i2, int size){
		return Math.min(Math.abs(i1 - i2), Math.min(i1, i2) + size - Math.max(i1, i2));
	}


	/**
	 *
	 * 解法： 暴力递归
	 * @param ring
	 * @param key
	 * @return
	 */
	public int findRotateSteps(String ring, String key) {

		char[] ringCharArray = ring.toCharArray();
		int rSize = ringCharArray.length;
		// ring 中每个字符出现的位置构建 map
		Map<Character, ArrayList<Integer>> map = new HashMap<>();
		for(int i = 0; i < rSize; i++){
			if(!map.containsKey(ringCharArray[i])){
				map.put(ringCharArray[i], new ArrayList<>());
			}
			map.get(ringCharArray[i]).add(i);
		}

		// 调用递归过程求解(深度优先遍历)
		int min = minSteps(0, 0, key.toCharArray(), map, rSize);
		return min;
	}

	/**
	 *
	 * @param preIndex
	 * @param keyIndex
	 * @param key
	 * @param map
	 * @param size
	 * @return
	 */
	public static int minSteps(int preIndex, int keyIndex, char[] key,
	                           Map<Character, ArrayList<Integer>> map,
	                           int size){
		// base case
		if(keyIndex == key.length){
			return 0;
		}
		int ans = Integer.MAX_VALUE;
		// 遍历每一个目标字符
		for(int curIndex: map.get(key[keyIndex])){
			// 顶针位置 + 1(button) + 下一步的最小步数
			int step = dial(preIndex, curIndex, size) + 1
					+ minSteps(curIndex, keyIndex + 1, key, map, size);
			ans =  Math.min(ans, step);
		}
		return ans;
	}


	// ===============================记忆化搜索优化递归================================================

	/**
	 *
	 * 解法： 暴力递归的优化，记忆化搜索
	 * @param ring
	 * @param key
	 * @return
	 */
	public static int findRotateSteps2(String ring, String key) {

		char[] ringCharArray = ring.toCharArray();
		int rSize = ringCharArray.length;
		// ring 中每个字符出现的位置构建 map
		Map<Character, ArrayList<Integer>> map = new HashMap<>();
		for(int i = 0; i < rSize; i++){
			if(!map.containsKey(ringCharArray[i])){
				map.put(ringCharArray[i], new ArrayList<>());
			}
			map.get(ringCharArray[i]).add(i);
		}
		// 递归中只有两个可变参数, preIndex和keyIndex
		int[][] dp = new int[rSize][key.length()+1];
		for(int i = 0; i < rSize; i++){
			for(int j = 0; j <= key.length(); j++){
				dp[i][j] = -1;
			}
		}
		// 调用递归过程求解(深度优先遍历)
		int min = minSteps2(0, 0, key.toCharArray(), map, rSize, dp);
		return min;
	}

	public static int minSteps2(int preIndex, int keyIndex, char[] key,
	                     Map<Character, ArrayList<Integer>> map,
	                     int size, int[][] dp){

		if(dp[preIndex][keyIndex] != -1){
			return dp[preIndex][keyIndex];
		}
		// base case
		if(keyIndex == key.length){
			dp[preIndex][keyIndex] = 0;
			return 0;
		}
		int ans = Integer.MAX_VALUE;
		// 遍历每一个目标字符
		for(int curIndex: map.get(key[keyIndex])){
			// 顶针位置 + 1(button) + 下一步的最小步数
			int step = dial(preIndex, curIndex, size) + 1
					+ minSteps2(curIndex, keyIndex + 1, key, map, size, dp);
			ans =  Math.min(ans, step);
		}
		dp[preIndex][keyIndex] = ans;
		return ans;
	}


	// =======================动态规划=============================
	/**
	 *
	 * 解法： 动态规划
	 * @param ring
	 * @param key
	 * @return
	 */
	public static int findRotateSteps3(String ring, String key) {

		char[] ringCharArray = ring.toCharArray();
		int rSize = ringCharArray.length;
		// ring 中每个字符出现的位置构建 map
		Map<Character, ArrayList<Integer>> map = new HashMap<>();
		for(int i = 0; i < rSize; i++){
			if(!map.containsKey(ringCharArray[i])){
				map.put(ringCharArray[i], new ArrayList<>());
			}
			map.get(ringCharArray[i]).add(i);
		}
		// 递归中只有两个可变参数, preIndex和keyIndex
		int[][] dp = new int[rSize][key.length()+1];
		for(int i = 0; i < rSize; i++){
			for(int j = 0; j <= key.length(); j++){
				dp[i][j] = -1;
			}
		}
		// 调用递归过程求解(深度优先遍历)
		int min = minSteps3(0, 0, key.toCharArray(), map, rSize, dp);
		return min;
	}


	public static int minSteps3(int preIndex, int keyIndex, char[] key,
	                     Map<Character, ArrayList<Integer>> map,
	                     int size, int[][] dp){

		// base case
		if(keyIndex == key.length){
			dp[preIndex][keyIndex] = 0;
		}
		int ans = Integer.MAX_VALUE;
		// 遍历每一个目标字符
		for(int i = 0; i < dp.length; i++){
			for(int j = 0; j < dp[i].length; j++){
				int step = dial(i, j, size) + 1
						+dp[j][i+1];

			}
		}
		for(int curIndex: map.get(key[keyIndex])){
			// 顶针位置 + 1(button) + 下一步的最小步数
			int step = dial(preIndex, curIndex, size) + 1
					+ dp[curIndex][keyIndex + 1];
			ans =  Math.min(ans, step);
		}
		dp[preIndex][keyIndex] = ans;
		return ans;
	}






}
