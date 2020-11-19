package com.dreams.algorithm.leetcode.simple;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/11/19 10:23
 * @description TODO
 */
public class Problem_283_MoveZeroes {

	public static void main(String[] args) {

	}


	/**
	 * Given an array nums, write a function to move all 0's to the end of it while
	 * maintaining the relative order of the non-zero elements.
	 *
	 * 来源：力扣（LeetCode）
	 * 链接：https://leetcode-cn.com/problems/move-zeroes
	 * 著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
	 * @param nums
	 */
	public void moveZeroes(int[] nums) {

		if(nums.length < 1){
			return;
		}

		int j = 0;
		for(int i = 0; i < nums.length; i++){
			if(nums[i] != 0){
				nums[j] = nums[i];
				j++;
			}
		}
		// 补零
		for(; j < nums.length; j++){
			nums[j] = 0;
		}
	}
}
