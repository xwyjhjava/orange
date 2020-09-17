package com.dreams.algorithm.sort;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.sort
 * @date 2020/9/17 18:57
 * @description 选择排序
 */
public class SelectSort extends AbstractSort {


	private int[] arr = {5, 6, 3, 7, 1, 9, 2};

	@Override
	public void setup() {
		super.setup();
		// 排序前
		System.out.println("排序前");
		for (int i = 0; i < arr.length; i++) {
			System.out.print(arr[i] + " ");
		}
		System.out.println("\n");
	}

	@Override
	public void sort() {

		//最小值index
		for (int i = 0; i < arr.length - 1; i++) {
			int minPos = i;
			for (int j = i + 1; j < arr.length; j++) {
				// 后面的值比前面的小
				if(arr[minPos] > arr[j]){
					minPos = j;
				}
				minPos = arr[j] < arr[minPos] ? j : minPos;
			}
			// 交换位置
			swap(arr, i, minPos);
			System.out.println("minPos = " + minPos);
		}
		//排序后
		for (int i = 0; i < arr.length; i++) {
			System.out.print(arr[i] + " ");
		}

	}


	public static void swap(int[] arr, int i, int j){
		int tmp = arr[i];
		arr[i] = arr[j];
		arr[j] = tmp;

	}


	public static void main(String[] args) {
		SelectSort selectSort = new SelectSort();
//		selectSort.setup();
//		selectSort.sort();


//		TODO 一次找出最小值和最大值
		int[] arr = {2, 3, 8, 9, 1};
		for (int i = 0; i < arr.length - 1; i++) {
			// 最小值
			int minPos = i;
			int maxPos = i;
			for (int j = i + 1; j < arr.length; j++) {
				// 比较
				minPos = arr[j] < arr[minPos] ? j : minPos;
				maxPos = arr[j] > arr[maxPos] ? j : maxPos;

			}
			System.out.println("minPos = " + arr[minPos]);
			System.out.println("maxPos = " + arr[maxPos]);

			swap(arr, i, minPos);
			swap(arr, arr.length - i - 1, maxPos);

			for (int k = 0; k < arr.length; k++) {
				System.out.print(arr[k] + " ");
			}
			System.out.println("\n");

			i--;
		}

		for (int i = 0; i < arr.length; i++) {
			System.out.println("i = " + arr[i]);
		}





	}


}
