package com.dreams.algorithm.leetcode.medium;

import java.util.Arrays;
import java.util.Comparator;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/11/16 11:33
 * @description TODO
 */
public class Problem_406_QueueReconstructionByHeight {

	public static void main(String[] args) {

	}

	/**
	 * Suppose you have a random list of people standing in a queue. Each person is described by a pair of integers (h, k),
	 * where h is the height of the person and k is the number of people in front of this person who have a height
	 * greater than or equal to h. Write an algorithm to reconstruct the queue.
	 *
	 * Note:
	 * The number of people is less than 1,100.
	 *
	 * 来源：力扣（LeetCode）
	 * 链接：https://leetcode-cn.com/problems/queue-reconstruction-by-height
	 * 著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
	 *
	 * 解法:
	 * @param people
	 * @return
	 */
	public static int[][] reconstructQueue(int[][] people) {
		// 先按照身高排序
		Arrays.sort(people[0]);


		return people;
	}


	/**
	 * 官方解法
	 * @param people
	 * @return
	 */
	public static int[][] reconstructQueue2(int[][] people){
		Arrays.sort(people, (person1, person2) -> {
			if(person1[0] != person2[0]){
				return person1[0] - person2[0];
			}else{
				return person2[1] - person1[1];
			}
		});

		int n = people.length;
		int[][] ans = new int[n][];
		for(int[] person : people){
			int spaces = person[1] + 1;
			for(int i = 0; i < n; ++i){
				if(ans[i] == null){
					--spaces;
					if(spaces == 0){
						ans[i] = person;
						break;
					}
				}
			}
		}
		return ans;
	}
}
