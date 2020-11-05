package com.dreams.algorithm.leetcode.simple;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/11/2 11:45
 * @description TODO
 */
public class Problem_349_IntersectionOfTwoArrays {


	public static void main(String[] args) {
		int[] nums1 = {1, 2};
		int[] nums2 = {2, 1};
		int[] intersection = intersection(nums1, nums2);
		for(int ele : intersection){
			System.out.println("ele = " + ele);
		}
	}

	public static int[] intersection(int[] nums1, int[] nums2) {

		if(nums1 == null || nums1.length < 1 || nums2 == null || nums2.length < 1){
			return nums1.length < 1 ? nums1 : nums2;
		}

		Arrays.sort(nums1);
		Arrays.sort(nums2);


		int length = Math.max(nums1.length, nums2.length);
		int[] ans = new int[length];

		int index1 = 0;
		int index2 = 0;
		int index = 0;
		// 双指针遍历两个数组
		while(index1 < nums1.length && index2 < nums2.length){
			int num1 = nums1[index1];
			int num2 = nums2[index2];
			// 如果相等， 则加到ans中
			if(num1 == num2){
				// 去重
				if(index == 0 || ans[index] != ans[index - 1]){
					ans[index++] = num1;
				}
				index1++;
				index2++;
			}else if(nums1.length > nums2.length){
				index2++;
			}else{
				index1++;
			}
		}
		return Arrays.copyOfRange(ans, 0, index);
	}



}
