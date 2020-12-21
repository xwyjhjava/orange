package com.dreams.algorithm.company;

import java.util.Arrays;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.company
 * @date 2020/11/19 16:38
 * @description TODO
 */
public class Problem_01_Jikai {
	// 一个数组中， 由正负两种数组成， 在额外空间复杂度是O(1)的情况下， 实现负数升序， 正数降序
	public static void main(String[] args) {
		int[] arr = {-1, -8, -10, -5, 9, 10, 2, 7, 12};
		sort(arr);
	}
	/**
	 *
	 * @param arr
	 */
	public static void sort(int[] arr){
		if(arr.length < 1){
			return;
		}

		// 标记 a 类的下标, 表示负数， 升序
		int indexA = 0;
		// 标记 b 类的下标, 表示正负， 降序
		int indexB = 0;
		for (int i = 0; i < arr.length; i++) {
			for(int j = i + 1 ; j < arr.length; j++){

				int cur = arr[i];
				// cur < 0 的逻辑判断可以替换为合同是否交付
				if(cur < 0){ // 表示当前元素是 a 类， 找到下一个 a 类坐标
					indexA = j;
					// arr[indexA] > 0的判断 替换为判断合同是否交付
					while(indexA < arr.length && arr[indexA] > 0){
						indexA++;
					}
					// cur > arr[indexA] 的判断替换为 交付时间的比较
					if(indexA < arr.length && cur > arr[indexA]){
						swap(arr, i, indexA);
					}
				}else{
					indexB = j;
					// arr[indexB] < 0的判断，替换为判断 合同是否交付
					while(indexB < arr.length && arr[indexB] < 0){
						indexB++;
					}
					// cur < arr[indexB]的判断， 替换为判断 启动时间的比较
					if(indexB < arr.length && cur < arr[indexB]){
						swap(arr, i, indexB);
					}

				}
			}
		}
		for(Integer ele : arr){
			System.out.print(" " + ele);
		}

	}

	public static void swap(int[] arr, int i, int j){
		int tmp = arr[i];
		arr[i] = arr[j];
		arr[j] = tmp;
	}





}
