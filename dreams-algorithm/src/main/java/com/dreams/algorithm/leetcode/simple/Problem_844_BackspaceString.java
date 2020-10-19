package com.dreams.algorithm.leetcode.simple;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/10/19 13:51
 * @description TODO
 */
public class Problem_844_BackspaceString {

	public static void main(String[] args) {
		String S = "bxj##tw";
		String T = "bxj###tw";

		boolean flag = backspaceCompare(S, T);
		System.out.println("flag = " + flag);
	}

	/**
	 * Given two strings S and T, return if they are equal when both are typed into empty text editors.
	 * # means a backspace character
	 *
	 * S and T only contain lowercase letters and '#' character
	 * 要求时间复杂度O(N), 额外空间复杂度O(1)
	 *
	 * 在不限制O(1)资源的情况下， 用stack很容易解这个问题
	 * @param S   1 <= S.length <= 200
	 * @param T   1 <= T.length <= 200
	 * @return
	 */
	// 从右向左遍历， 适合该题
	public static boolean backspaceCompare(String S, String T){

		int sIndex = S.length() - 1;
		int tIndex = T.length() - 1;

		int sSpaceCount = 0;
		int tSpaceCount = 0;

		while(sIndex >= 0 || tIndex >= 0){
			// 如果是字符， 则拿出来比较， 否则spaceCount++, index--
			while(sIndex >= 0){
				if('#' == S.charAt(sIndex)){
					sSpaceCount++;
				// 是字符，且spaceCount == 0, 则跳出， 比较当前字符
				}else if(sSpaceCount == 0){
					break;
				// 是字符， 但spaceCount != 0, 则spaceCount--
				}else{
					sSpaceCount--;
				}
				sIndex--;
			}
			// sIndex 是当前 S串出现的第一个字符

			while(tIndex >= 0){
				if('#' == T.charAt(tIndex)){
					tSpaceCount++;
					// 是字符，且spaceCount == 0, 则跳出， 比较当前字符
				}else if(tSpaceCount == 0){
					break;
					// 是字符， 但spaceCount != 0, 则spaceCount--
				}else{
					tSpaceCount--;
				}
				tIndex--;
			}
			// tIndex 是当前 T串出现的第一个字符


			// S 和 T 同时都遍历结束了
			// 过滤了sIndex = -1, tIndex = -1 的情况
			// 这种情况， 说明S，T字符被删光了， 都为空
			if(sIndex < 0 && tIndex < 0){
				return true;
			}

			// 说明 此时 sIndex 和 tIndex至少有一个 >=0
			// sIndex 和 tIndex 都 >= 时， 说明有字符需要比较
			if(sIndex >= 0 && tIndex >= 0) {
				//如果字符相等， 则sIndex--, tIndex--
				if (S.charAt(sIndex) == T.charAt(tIndex)) {
					sIndex--;
					tIndex--;

					// 当sIndex=0 && tIndex=0时， 说明字符比较完了
					// 过滤了 sIndex=-1 && tIndex=-1的情况
					if(sIndex < 0 && tIndex < 0){
						return true;
					}
				// 字符不匹配， 直接返回false
				} else {
					break;
				}
			// 只有当sIndex和tIndex是 0和-1的组合时，才会走到这个else分支
			}else{
				break;
			}
		}
		return false;
	}



}
