package com.dreams.algorithm.leetcode.simple;

import java.util.*;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/11/14 23:24
 * @description TODO
 */
public class Problem_1122_RelativeSortArray {

	public static void main(String[] args) {
		int[] arr1 = {2,21,43,38,0,42,33,7,24,13,12,27,12,24,5,23,29,48,30,31};
		int[] arr2 = {2,42,38,0,43,21};
		int[] ans = relativeSortArray(arr1, arr2);
		for(Integer ele : ans){
			System.out.print(ele + " ");
		}
	}

	/**
	 *
	 * Given two arrays arr1 and arr2, the elements of arr2 are distinct, and all elements in arr2 are also in arr1.
	 *
	 * Sort the elements of arr1 such that the relative ordering of items in arr1 are the same as in arr2.  Elements that don't appear in arr2 should be placed at the end of arr1 in ascending order.
	 *
	 * 解法： 暴力解
	 * @param arr1
	 * @param arr2
	 *      arr1.length, arr2.length <= 1000
	 *      0 <= arr1[i], arr2[i] <= 1000
	 *      Each arr2[i] is distinct.
	 *      Each arr2[i] is in arr1.
	 * @return
	 */
	public static int[] relativeSortArray(int[] arr1, int[] arr2) {

		Map<Integer, Integer> arr1Map = new HashMap<Integer, Integer>();
		// 统计 arr1中每个数字出现的次数
		for(Integer ele : arr1){
			if(arr1Map.containsKey(ele)){
				arr1Map.put(ele, arr1Map.get(ele) + 1);
			}else{
				arr1Map.put(ele, 1);
			}
		}

		int[] result = new int[arr1.length];
		int index = 0;
		// 遍历arr2, 封装result
		for(Integer ele : arr2){
			int count = arr1Map.get(ele);
			for(; count > 0; count--){
				result[index] = ele;
				index++;
			}
			// 从map中去掉当前值
			arr1Map.remove(ele);
		}
		// arr1Map中剩下的数进行升序排序
		if(result.length == arr2.length){
			return result;
		}

		int[] remain = new int[arr1.length - index];
		int remainIndex = 0;
		for(Map.Entry<Integer, Integer> ele : arr1Map.entrySet()){
			Integer key = ele.getKey();
			Integer value = ele.getValue();
			for(; value > 0; value--){
				remain[remainIndex] = key;
				remainIndex++;
			}
		}
		// 升序排序
		Arrays.sort(remain);
		for(int i = 0; i < remain.length; i++){
			result[index] = remain[i];
			index++;
		}
		return result;
	}


	/**
	 * 优化解法： 计数排序
	 * @param arr1
	 * @param arr2
	 * @return
	 */
	public int[] relativeSortArray2(int[] arr1, int[] arr2) {
		// 找到arr1的最大值
		int max = 0;
		for(int ele : arr1){
			max = max >= ele ? max : ele;
		}
		int[] help = new int[max + 1];
		for(int ele : arr1){
			help[ele] += 1;
		}
		int[] ans = new int[arr1.length];
		int ansIndex = 0;
		for(int ele : arr2){
			int count = help[ele];
			while(count > 0){
				ans[ansIndex] = ele;
				count--;
				ansIndex++;
			}
			// 标记
			help[ele] = 0;
		}
		for(int index = 0; index < help.length; index++){
			int count = help[index];
			while (count > 0){
				ans[ansIndex] = index;
				ansIndex++;
				count--;
			}
		}
		return ans;
	}




}
