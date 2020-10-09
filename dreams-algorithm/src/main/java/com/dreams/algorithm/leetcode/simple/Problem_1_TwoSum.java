package com.dreams.algorithm.leetcode.simple;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/10/3 21:16
 * @description TODO
 */
public class Problem_1_TwoSum {

	public static void main(String[] args) {

	}

	/**
	 * 给定一个数组和目标target， 在数组中两个唯一解的两数之和是target的数组下标
	 * @param nums
	 * @param target
	 * @return
	 */
	public int[] twoSum(int[] nums, int target) {
		int[] result = new int[2];
		Map<Integer, Integer> numsMap = new HashMap<>();
		for (int i = 0; i < nums.length; i++) {
			// target - nums[i]
			int tmp = target - nums[i];
			// map 中 找不到
			if(numsMap.get(tmp) == null){
				//相同的数字如果是唯一解，那么target必然是两个相同数字的和
				numsMap.put(nums[i], i);
			}else{  // map 中找到了
				result[0] = numsMap.get(tmp);
				result[1] = i;
				break;
			}
		}
		return result;
	}
}
