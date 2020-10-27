package com.dreams.algorithm.leetcode.medium;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/10/26 13:35
 * @description TODO
 */
public class Problem_845_LongestMountain {

	public static void main(String[] args) {
		int[] A = {2, 1, 4, 7, 3, 2, 5};
//		int[] A = {2, 2, 2};
		int max = longestMountain(A);
		System.out.println("max = " + max);
	}

	/**
	 * Let's call any (contiguous) subarray B (of A) a mountain if the following properties hold:
	 *      B.length >= 3
	 *      There exists some 0 < i < B.length - 1 such that B[0] < B[1] < ... B[i-1] < B[i] > B[i+1] > ... > B[B.length - 1]
	 *
	 * (Note that B could be any subarray of A, including the entire array A.)
	 * Given an array A of integers, return the length of the longest mountain. 
	 * Return 0 if there is no mountain.
	 *
	 * 暴力解法：  枚举山顶， 先找到比左右两边都大的数，依次左右两边扩充
	 * @param A   0 <= A.length <= 10000
	 *            0 <= A[i] <= 10000
	 * @return
	 */
	public static int longestMountain(int[] A){
		if(A.length < 3){
			return 0;
		}
		int mid = 1;
		// B.length >= 3
		// 划出一个窗口大小 == 3
		int left = 0;
		int right = 2;
		int max = 0;
		while(mid < A.length - 1){
			// 符合山脉规则
			if(A[left] < A[mid] && A[mid] > A[right]){
				// 往左找
				while(left > 0 && A[left] > A[left - 1]){
					left = left - 1;
				}
				// 往右找
				while(right < A.length - 1 && A[right] > A[right + 1]){
					right = right + 1;
				}
				max = Math.max(max, right - left + 1);
				// 更新mid
				mid++;
				left = mid - 1;
				right = mid + 1;
			}else{

				left++;
				mid++;
				right++;
			}

			// 如果max == A.length， 直接return, A.length已经是最大值了
			// 在某些case里， 可以省掉后面无用的遍历
			if(A.length == max){
				return max;
			}
		}
		return max;
	}


}
