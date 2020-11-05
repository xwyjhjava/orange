package com.dreams.algorithm.leetcode.simple;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/11/3 10:51
 * @description TODO
 */
public class Problem_941_ValidMountainArray {


	public static void main(String[] args) {
		int[] A = {0, 3, 2, 1};
		validMountainArray(A);
	}


	/**
	 * Given an array A of integers, return true if and only if it is a
	 * valid mountain array.
	 *
	 * @param A  0 <= A.length <= 10000
	 *           0 <= A[i] <= 10000
	 * @return
	 *
	 * 解法： 遍历找到山顶, 山顶往左到index=0, 山顶往右到index=length-1
	 */
	public static boolean validMountainArray(int[] A){
		// 找山顶
		int mid = 1;
		while(mid < A.length - 1){

			if(A[mid - 1] < A[mid] && A[mid] > A[mid + 1]){
				// 找到山顶
				int left = mid - 1;
				int right = mid + 1;
				// 往左 递增
				while(left > 0){
					if(A[left] > A[left - 1]){
						left--;
					}else{
						return false;
					}
				}
				// 往右 递减
				while(right < A.length - 1){
					if(A[right + 1] < A[right]){
						right++;
					}else{
						return false;
					}
				}
				return true;

			}else{
				mid++;
			}
		}
		return false;
	}


	/**
	 * 双指针
	 * @param A
	 * @return
	 */
	public static boolean validMountainArray2(int[] A){
		if(A.length < 3){
			return false;
		}
		// 左指针
		int left = 0;
		// 右指针
		int right = A.length - 1;

		// 递增趋势
		while(left < A.length - 1 && A[left] < A[left + 1]){
			left++;
		}
		// 递减趋势
		while(right > 0 && A[right] < A[right - 1]){
			right--;
		}
		// 山顶是否在同一个位置
		if(left == right && left > 0 && right < A.length - 1){
			return true;
		}
		return false;
	}


}
