package com.dreams.algorithm.leetcode.medium;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/12/1 11:24
 * @description TODO
 */
public class Problem_34_FindFirstAndLastPosition {

	public static void main(String[] args) {
		int[] nums = {5, 7, 7, 8, 8, 10};
		int target = 8;
//		int[] result = searchRange(nums, target);
//		System.out.println("result = " + result[0] + " " + result[1]);
//		binarySearch(nums, 8);
	}

	/**
	 * Given an array of integers nums sorted in ascending order, find the starting
	 * and ending position of a given target value.
	 *
	 * If target is not found in the array, return [-1, -1].
	 *
	 * Follow up: Could you write an algorithm with O(log n) runtime complexity?
	 *
	 * 来源：力扣（LeetCode）
	 * 链接：https://leetcode-cn.com/problems/find-first-and-last-position-of-element-in-sorted-array
	 * 著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
	 *
	 * 解法: 二分查找
	 * @param nums
	 * @param target
	 * @return
	 */
	public static int[] searchRange(int[] nums, int target){

		int[] ans = new int[]{-1, -1};
		if(nums.length < 1){
			return ans;
		}

		int leftIndex = findFirstPosition(nums, target);
		if(leftIndex == -1){
			return ans;
		}
		int rightIndex = findLastPosition(nums, target);
		return new int[]{leftIndex, rightIndex};
	}


	// 二分查找
	public static int findFirstPosition(int[] nums, int target){
		int left = 0;
		int right = nums.length - 1;
		while(left < right){
			// >>> 是无符号右移
			int mid = (left + right) >>> 1;

			if(nums[mid] > target){
				// 下一轮搜索区间 [left, mid - 1]
				right = mid - 1;
			}else if(nums[mid] == target){
				// 下一轮搜索区间 [left, mid] 或者 [mid, right]
				right = mid;
			}else{
				// 下一轮搜索区间 [mid + 1, right]
				left = mid + 1;
			}
		}
		if(nums[left] == target){
			return left;
		}
		return -1;
	}


	public static int findLastPosition(int[] nums, int target){
		int left = 0;
		int right = nums.length - 1;
		while(left < right){
			int mid = (left + right + 1) >>> 1;
			if(nums[mid] > target){
				// 下一轮搜索区间 [left, mid - 1]
				right = mid - 1;
			}else if(nums[mid] == target){
				// 下一轮搜索区间 [mid, right]
				left = mid;
			}else{
				// 下一轮搜索区间 [mid + 1, right]
				left = mid + 1;
			}
		}
		return left;
	}



	// ==========================leetcode 管方解法===========================

	public static int[] searchRange_2(int[] nums, int target){
		int leftIdx = binarySearch_2(nums, target, true);
		int rightIdx = binarySearch_2(nums, target, false) - 1;
		if (leftIdx <= rightIdx && rightIdx < nums.length && nums[leftIdx] == target && nums[rightIdx] == target) {
			return new int[]{leftIdx, rightIdx};
		}
		return new int[]{-1, -1};
	}


	public static int binarySearch_2(int[] nums, int target, boolean lower) {
		int left = 0, right = nums.length - 1, ans = nums.length;
		while (left <= right) {
			int mid = (left + right) / 2;
			if (nums[mid] > target || (lower && nums[mid] >= target)) {
				right = mid - 1;
				ans = mid;
			} else {
				left = mid + 1;
			}
		}
		return ans;
	}

}
