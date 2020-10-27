package com.dreams.algorithm.leetcode.simple;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/10/26 10:52
 * @description TODO
 */
public class Problem_1365_NumberSmall {


	public static void main(String[] args) {
		int[] nums = {5, 0, 10, 0, 10, 6};
		smallerNumbersThanCurrentByCountSort(nums);

	}

//

//	Return the answer in an array.


	/**
	 * Given the array nums, for each nums[i] find out how many numbers in the array are smaller than it.
	 * That is, for each nums[i] you have to count the number of valid j's such that j != i and nums[j] < nums[i].
	 * Return the answer in an array.
	 *
	 * 暴力解法， 时间复杂度O(N^2), 额外空间复杂度 O(1)
	 * @param nums  2 <= nums.length <= 500
	 *              0 <= nums[i] <= 100
	 * @return
	 */
	public int[] smallerNumbersThanCurrent(int[] nums) {
		int length = nums.length;
		int[] ans = new int[length];

		for (int i = 0; i < length; i++) {
			int count = 0;
			for(int j = 0; j < length; j++){
				// nums[i] != nums[j] 且 nums[i] > nums[j]
				if(nums[i] != nums[j] && nums[i] > nums[j]){
					count++;
				}
			}
			ans[i] = count;
		}
		return ans;
	}


	/**
	 * 计数排序
	 * @param nums
	 * @return
	 */
	public static int[] smallerNumbersThanCurrentByCountSort(int[] nums){
		// 找到数组最大值
		int max = 0;
		for (int i = 0; i < nums.length; i++) {
			max = max <= nums[i] ? nums[i]: max;
		}
		// 0 <= nums[i] <= max
		// 申请辅助数组
		int[] help = new int[max + 1];
		// 计数
		for (int i = 0; i < nums.length; i++) {
			help[nums[i]]++;
		}
		int[] ans = new int[nums.length];
		for (int i = 1; i < help.length; i++) {
			// 当前 i 的count值
			help[i] += help[i - 1];
		}
		for (int i = 0; i < nums.length; i++) {
			ans[i] = nums[i] == 0 ? 0 : help[nums[i] - 1];
		}
		return ans;
	}

}
