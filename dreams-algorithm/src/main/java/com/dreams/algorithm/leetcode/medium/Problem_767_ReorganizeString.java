package com.dreams.algorithm.leetcode.medium;


import java.util.PriorityQueue;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/11/30 10:45
 * @description TODO
 */
public class Problem_767_ReorganizeString {

	public static void main(String[] args) {

	}


	/**
	 *
	 * Given a string S, check if the letters can be rearranged so that two characters that are adjacent
	 * to each other are not the same.
	 *
	 * If possible, output any possible result.  If not possible, return the empty string.
	 *
	 * 来源：力扣（LeetCode）
	 * 链接：https://leetcode-cn.com/problems/reorganize-string
	 * 著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
	 *
	 * 解法： 基于计数的贪心
	 * @param S
	 * @return
	 */
	public String reorganizeString(String S){

		if(S.length() < 2){
			return S;
		}

		// 只包含小写字母
		int[] help = new int[26];
		char[] arr = S.toCharArray();
		int length = arr.length;
		int maxLength = 0;
		// 预处理
		for(int i = 0; i < arr.length; i++){
			help[arr[i] - 'a']++;
			maxLength = Math.max(help[arr[i] - 'a'], maxLength);
		}
		if(maxLength > (arr.length + 1) / 2){
			return "";
		}

		char[] reorganizeArray = new char[length];
		int evenIndex = 0, oddIndex = 1;
		int halfLength = length / 2;
		for (int i = 0; i < 26; i++) {
			char c = (char) ('a' + i);
			while (help[i] > 0 && help[i] <= halfLength && oddIndex < length) {
				reorganizeArray[oddIndex] = c;
				help[i]--;
				oddIndex += 2;
			}
			while (help[i] > 0) {
				reorganizeArray[evenIndex] = c;
				help[i]--;
				evenIndex += 2;
			}
		}
		return new String(reorganizeArray);
	}


	/**
	 * 解法： 基于大根堆的贪心
	 * @param S
	 * @return
	 */
	public String reorganizeString_2(String S){
		if(S.length() < 2){
			return S;
		}
		int[] counts = new int[26];
		char[] arr = S.toCharArray();
		int length = arr.length;
		// 最多频次的字符
		int maxLength = 0;
		for(int i = 0; i < length; i++){
			counts[arr[i] - 'a']++;
			maxLength = Math.max(counts[arr[i] - 'a'], maxLength);
		}
		if(maxLength > (length + 1) / 2){
			return "";
		}

		// 申请大根堆
		PriorityQueue<Character> queue = new PriorityQueue<>((o1, o2) -> counts[o2 - 'a'] - counts[o1 - 'a']);

		for(char tmp = 'a'; tmp <= 'z'; tmp++){
			if(counts[tmp - 'a'] > 0){
				queue.offer(tmp);
			}
		}

		StringBuilder builder = new StringBuilder();

		// 保证堆内至少有两个元素
		while(queue.size() > 1){
			// 弹出两个堆顶
			Character letter1 = queue.poll();
			Character letter2 = queue.poll();
			builder.append(letter1).append(letter2);

			counts[letter1 - 'a']--;
			counts[letter2 - 'a']--;

			if(counts[letter1 - 'a'] > 0){
				queue.offer(letter1);
			}
			if(counts[letter2 - 'a'] > 0){
				queue.offer(letter2);
			}
		}
		if(queue.size() > 0){
			builder.append(queue.poll());
		}

		return builder.toString();
	}


}
