package com.dreams.algorithm.leetcode.simple;

import java.util.*;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/11/6 11:02
 * @description TODO
 */
public class Problem_1356_SortIntegersByBits {

	public static void main(String[] args) {
		int[] arr = {0,1,2,3,4,5,6,7,8};
		sortByBits(arr);
	}

	public static int[] sortByBits(int[] arr){
		int[] bit = new int[10001];
		List<Integer> list = new ArrayList<Integer>();
		for (int x : arr) {
			list.add(x);
			bit[x] = Integer.bitCount(x);
		}
		Collections.sort(list, (x, y) -> {
			if (Integer.bitCount(x) != Integer.bitCount(y)) {
				return Integer.bitCount(x) - Integer.bitCount(y);
			} else {
				return x - y;
			}
		});
		for (int i = 0; i < arr.length; ++i) {
			arr[i] = list.get(i);
		}
		return arr;
	}


}
