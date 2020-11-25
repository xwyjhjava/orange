package com.dreams.algorithm.sort;

import java.util.Arrays;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.sort
 * @date 2020/11/20 10:50
 * @description TODO
 */
public class InsertSort {

	public static void main(String[] args) {
		int[] arr = {1,2,7,9,4};
		sort(arr);
	}

	public static void sort(int[] arr){
		if(arr == null || arr.length < 2){
			return;
		}

		for(int i = 1; i < arr.length; i++){
			for(int j = i - 1; j < arr.length; j--){
				if(arr[j] > arr[j + 1]){
					swap(arr, j, j + 1);
				}else{
					break;
				}
			}
		}

		for (int i = 0; i < arr.length; i++) {
			System.out.print(" " + arr[i]);
		}
	}

	public static void swap(int[] arr, int i, int j){
		int tmp = arr[i];
		arr[i] = arr[j];
		arr[j] = tmp;
	}
}
