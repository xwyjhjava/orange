package com.dreams.algorithm.sort;

import sun.applet.AppletResourceLoader;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.sort
 * @date 2020/10/12 11:53
 * @description 归并排序
 */
public class MergeSort {

	public static void main(String[] args) {
		int[] arr = {2, 9, 8, 3, 1};
//		mergeSort1(arr, 0, arr.length - 1);
//		mergeSort2(arr);
		int result = smallSum(arr);
		System.out.println("result = " + result);
		for (int i = 0; i < arr.length; i++) {
			System.out.println("i = " + arr[i]);
		}
	}

	// 递归实现
	public static void mergeSort1(int[] arr, int L, int R){
		//base case
		if(L == R){
			return;
		}
		int mid = L + ((R - L) >> 1); // mid = L + (R -L) / 2
		mergeSort1(arr, L, mid);
		mergeSort1(arr, mid + 1, R);
		merge(arr, L, mid, R);
	}
	public static void merge(int[] arr, int L, int M, int R){
		//申请一个辅助数组
		int[] help = new int[R - L + 1];
		// i 给help 数组使用
		int i = 0;
		// 左边部分第一个数
		int left = L;
		// 右边部分第一个数
		int right = M + 1;
		// 左右都不越界， 下标依次向后移
		while(left <= M && right <= R){
			// 左右部分谁小，就先拷贝谁
			if(arr[left] <= arr[right]){
				help[i] = arr[left];
				left++;
			}else{
				help[i] = arr[right];
				right++;
			}
			i++;
		}
		// 左或者右越界时
		// 当上面的while条件不满足时， 下面的两个while必会走一个
		while(left <= M){
			help[i] = arr[left];
			i++;
			left++;
		}
		while(right <= R){
			help[i] = arr[right];
			i++;
			right++;
		}
		// help 数组的数写回arr
		for(i = 0; i < help.length; i++){
			arr[L + i] = help[i];
		}
	}


	// 非递归实现
	public static void mergeSort2(int[] arr){
		if(arr == null || arr.length < 2){
			return;
		}
		int length = arr.length;
		// mergeSize 表示划分的左右部分大小，一个组包含左右两个部分，大下是mergeSize*2
		int mergeSize = 1;
		while(mergeSize < length){
			int left = 0;
			// 找出每组数
			while(left < length){
				// 中点
				int mid = left + mergeSize - 1;
				// mid 越界, 说明最后几个数不能构成一个左部分
				if(mid >= length){
					break;
				}
				// 右边界
				int right = Math.min(mid + mergeSize, length - 1);
				merge(arr, left, mid, right);

				left = right + 1;
			}
			// 防止mergeSize<<1 时下标越界
			if(mergeSize > length / 2){
				break;
			}
			mergeSize <<= 1;
		}
	}


	/**
	 * 求一个数组的数组小和
	 * @param arr
	 * @return
	 */
	public static int smallSum(int[] arr){
		if(arr == null || arr.length < 2){
			return 0;
		}
		return process(arr, 0, arr.length - 1);
	}

	/**
	 * arr[L...R] 既要排好序，也要求小和返回
	 * @param arr
	 * @param left
	 * @param right
	 * @return
	 */
	public static int process(int[] arr, int left, int right){
		if(left == right){
			return 0;
		}
		int mid = left + ((right - left) >> 1);
		int leftRes = process(arr, left, mid);
		int rightRes = process(arr, mid + 1, right);
		int mergeRes = mergeSmallSum(arr, left, mid, right);
		return leftRes + rightRes + mergeRes;
	}

	public static int mergeSmallSum(int[] arr, int L, int M, int R){
		int[] help = new int[R - L + 1];
		int i = 0;
		int p1 = L;
		int p2 = M + 1;
		int res = 0;
		while(p1 <= M && p2 <= R){
			// 当左组比右组的值小时， 符合要求， 算小数和。
			res += arr[p1] < arr[p2] ? (R - p2 + 1) * arr[p1] : 0;
			// 当左组的数==右组的数时， 先填右组的数， 再p2++
			if(arr[p1] < arr[p2]){
				help[i] = arr[p1];
				p1 ++;
			}else{
				help[i] = arr[p2];
				p2 ++;
			}
			i++;
		}
		while(p1 <= M){
			help[i] = arr[p1];
			i++;
			p1++;
		}
		while(p2 <= R){
			help[i] = arr[p2];
			i++;
			p2++;
		}
		// 数据写回arr
		for (i = 0; i < help.length; i++){
			arr[L + i] = help[i];
		}
		return res;
	}






}
