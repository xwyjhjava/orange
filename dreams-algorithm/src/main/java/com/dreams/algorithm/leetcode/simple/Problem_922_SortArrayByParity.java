package com.dreams.algorithm.leetcode.simple;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/11/12 10:48
 * @description TODO
 */
public class Problem_922_SortArrayByParity {

	public static void main(String[] args) {

	}

	/**
	 * Given an array A of non-negative integers, half of the integers in A are odd, and half of the integers are even.
	 *
	 * Sort the array so that whenever A[i] is odd, i is odd; and whenever A[i] is even, i is even.
	 *
	 * You may return any answer array that satisfies this condition.
	 *
	 * @param A
	 * @return
	 */
	public int[] sortArrayByParityII(int[] A){

		int evenIndex = 0;
		int oddIndex = 1;

		for(; evenIndex < A.length - 1 && oddIndex < A.length; evenIndex += 2){
			if(A[evenIndex] % 2 == 1){
				swap(evenIndex, oddIndex, A);
				oddIndex += 2;
				// evenIndex 停住
				evenIndex -= 2;
			}
		}
		return A;
	}

	public static void swap(int i, int j, int[] A){
		int tmp = A[i];
		A[i] = A[j];
		A[j] = tmp;
	}


}
