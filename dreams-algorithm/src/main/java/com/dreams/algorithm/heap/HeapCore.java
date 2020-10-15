package com.dreams.algorithm.heap;

import java.util.PriorityQueue;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.heap
 * @date 2020/10/13 18:45
 * @description TODO
 */
public class HeapCore {

	public static void main(String[] args) {

	}


	public static void headSort(int[] arr, int k){
		PriorityQueue<Integer> heap = new PriorityQueue<>();
		// arr.length 和 k 的最小值
		int limitSize = Math.min(arr.length - 1, k);
		int index = 0;
		// 构建 k的小根堆
		for (; index < limitSize; index++) {
			heap.add(arr[index]);
		}

		int i = 0;
		// 小根堆poll， 再加入新的元素, 加到没元素加时， 跳出循环
		for(; index < arr.length; i++, index++){
			arr[i] = heap.poll();
			heap.add(arr[index]);
		}

		// 此时如果小根堆不为空， 则依次poll
		while(!heap.isEmpty()){
			arr[i] = heap.poll();
		}
	}



}
