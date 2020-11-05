package com.dreams.algorithm.leetcode.hard;

import java.util.*;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.hard
 * @date 2020/11/4 14:56
 * @description TODO
 */
public class Problem_57_InsertInterval {

	public static void main(String[] args) {
		int[][] interval = new int[][]{
				{1, 3},{6, 9}
		};
		int[] newInterval = {2, 5};
		insert(interval, newInterval);
	}

	/**
	 *
	 * Given a set of non-overlapping intervals, insert a new interval into the intervals (merge if necessary).
	 *
	 * You may assume that the intervals were initially sorted according to their start times.
	 *
	 *
	 * 解法： 时间复杂度 O(NlogN) , 用problem_56的思路
	 *       此题的时间复杂度可以到O(N)
	 * @param intervals
	 * @param newInterval
	 * @return
	 */
	public static int[][] insert(int[][] intervals, int[] newInterval) {
		if(intervals.length < 1 || newInterval.length < 1){
			return intervals.length < 1 ? new int[][]{newInterval}: intervals;
		}
		// 合并两个区间
		List<int[]> list = new ArrayList<>(Arrays.asList(intervals));
		list.add(newInterval);
		// 按照interval的star 排序
		Collections.sort(list, Comparator.comparingInt(v -> v[0]));
		List<int[]> ans = new ArrayList<>();
		// 以下 list 取代 intervals
		ans.add(list.get(0));
		int preIntervalEndValue = list.get(0)[1];

		for(int i = 1; i < list.size(); i++){
			// 当前interval
			int[] cur = list.get(i);
			// 不合并
			if(cur[0] > preIntervalEndValue){
				preIntervalEndValue = cur[1];
				ans.add(cur);
			}else{ // 合并
				// 更新 preIntervalEndValue 和 ans
				preIntervalEndValue = Math.max(preIntervalEndValue, cur[1]);
				ans.get(ans.size() - 1)[1] = preIntervalEndValue;
			}
		}
		return ans.toArray(new int[ans.size()][]);
	}
}
