package com.dreams.algorithm.leetcode.simple;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/12/3 13:49
 * @description TODO
 */
public class Problem_204_CountPrimes {

	public static void main(String[] args) {
		int n = 499979;
		int ans = countPrimes(n);
		System.out.println("ans = " + ans);
	}

	/**
	 * Count the number of prime numbers less than a non-negative number, n.
	 * 暴力解法超时
	 * @param n
	 * @return
	 */
	public static int countPrimes(int n){

		// 暴力方法
		int ans = 0;
		if(n <= 2){
			return ans;
		}
//		if(n == 3){
//			return 1;
//		}
		while(n > 2){
			boolean flag = true;
			int half = n >>> 1;
			for(int i = 2; i <= half; i++){
				if((n - 1) % i == 0){
					flag = false;
					break;
				}
			}
			if(flag){
				ans++;
			}
			n--;
		}
		return ans;
	}


	public static int countPrimes2(int n){

		int ans = 0;
		while(n > 2){
			boolean flag = true;
			for(int i = 2; i <= 9 && i != n; i++){
				if(n % i == 0){
					flag = false;
					break;
				}
			}

			if(flag){
				ans++;
			}
			n--;
		}
		return ans;
	}
}
