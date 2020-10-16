package com.dreams.algorithm.leetcode.simple;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/10/16 10:36
 * @description TODO
 */
public class Problem_977_SquaresSortedArray {

	public static void main(String[] args) {
		int[] arr = {4, 6, 1, 5, 9, 3};
		mergeSort(arr);
		printArray(arr);

	}
	/**
	 * Given an array of Integers A sorted in non-decreasing order,
	 * return an array of the squares of each number, also in sorted
	 * non-decreasing order.
	 *
	 * 数组本身是升序的，正数部分平方后，次序不变
	 * @param A
	 * @return
	 */
	public int[] sortedSquares(int[] A){
		// 双指针解法
		if(A == null){
			return null;
		}
		int[] help = new int[A.length];
		// helpIndex为help数组服务
		int helpIndex = A.length - 1;
		for(int i = 0, j = A.length - 1; i <= j; helpIndex--) {
			// 比较左右值，谁大谁就入help(逆序)
			if (A[i] * A[i] > A[j] * A[j]) {
				help[helpIndex] = A[i] * A[i];
				// i++
				i++;
			} else {
				help[helpIndex] = A[j] * A[j];
				//j--
				j--;
			}
			helpIndex--;
		}
		return help;
	}


	// 冒泡排序, 时间复杂地O(N^2), 额外空间复杂度O(1)
	public static void bubbleSort(int[] A){
		if(A == null || A.length < 2){
			return;
		}
		for(int i = 0; i < A.length; i++){
			for(int j = i + 1;  j < A.length; j++){
				// 比较大小
				if(A[i] > A[j]){
					swap(A, i, j);
				}
			}
		}
	}

	// 选择排序 时间复杂地O(N^2), 额外空间复杂度O(1)
	public static void selectedSort(int[] A){
		if(A == null || A.length < 2){
			return;
		}
		for(int i = 0; i < A.length; i++){
			int minPos = i;
			for(int j = minPos + 1; j < A.length; j++){
				if(A[minPos] > A[j]){
					// 更新小值
					minPos = j;
				}
			}
			// 交换
			swap(A, i, minPos);
		}
	}

	// 插入排序 时间复杂度O(N^2), 额外空间复杂度O(1)
	public static void insertSort(int[] A){
		if(A == null || A.length < 2){
			return;
		}
		// i 从第一个数开始
		for(int i = 1; i < A.length; i++){
			for(int j = i - 1; j >= 0 && A[j + 1] < A[j]; j--){
				swap(A, j + 1, j);
			}
		}
	}


	// 归并排序 , 时间复杂度O(N * logN)
	public static void mergeSort(int[] A){
		if(A == null || A.length < 2){
			return;
		}
		process(A, 0, A.length - 1);
	}
	public static void process(int[] arr, int L, int R){
		// L == R 时是临界条件
		if(L == R){
			return;
		}
		int mid = L + ((R -L) >> 1);
		process(arr, L, mid);
		process(arr, mid + 1, R);
		merge(arr, L, mid, R);
	}
	public static void merge(int[] arr, int L, int M, int R){
		int[] help = new int[R - L + 1];
		int i = 0;
		// 左边第一个数
		int left = L;
		// 右边第一个数
		int right = M + 1;

		// 左右下标都不越界
		while(left <= M && right <= R){
			// 比较左右两部分的数， 谁小就先进help数组
			if(arr[left] <= arr[right]){ // 相等时，先copy左边
				help[i] = arr[left];
				left++;
			}else{
				help[i] = arr[right];
				right++;
			}
			i++;
		}
		// 左边或者右边越界时， 将另外一边全部进数组
		while(left <= M){
			help[i] = arr[left];
			left++;
			i++;
		}
		while(right <= R){
			help[i] = arr[right];
			right++;
			i++;
		}
		//help 写回arr
		for (int j = 0; j < help.length; j++) {
			arr[L + j] = help[j];
		}
	}


	// 数据两数交换
	public static void swap(int[] A, int x, int y){
		int temp = A[x];
		A[x] = A[y];
		A[y] = temp;
	}

	// 数组打印
	public static void printArray(int[] arr){
		System.out.println("=========================");
		for (int i = 0; i < arr.length; i++) {
			System.out.print(arr[i] + " ");
		}
	}


}
