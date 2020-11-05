package com.dreams.algorithm.leetcode.medium;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/11/4 11:27
 * @description TODO
 */
public class Problem_56_MergeIntervals {

	public static void main(String[] args) {

		List<int[]> intervals = new ArrayList<int[]>(){{
			add(new int[]{1, 4});
			add(new int[]{0, 4});
//			add(new int[]{8, 10});
//			add(new int[]{15, 18});
		}};
		merge(intervals.toArray(new int[intervals.size()][]));
	}

	/**
	 * Given a collection of intervals, merge all overlapping intervals.
	 *
	 * 解法： 先将数组intervals按照start升序排, 再在排序的数组上解决
	 * @param intervals
	 * @return
	 */
	public static int[][] merge(int[][] intervals){

		if(intervals.length < 1){
			return intervals;
		}
		Arrays.sort(intervals, Comparator.comparingInt(v -> v[0]));
		List<int[]> ans = new ArrayList<>();
		int preVal = 0;

		for(int i = 0; i < intervals.length; i++){
			int[] cur = intervals[i];
			// 如果第一个的右>=第二个的左, 说明区间有重叠, 需要合并
			// 如果集合只有一个元素 或者 一右 < 二左, 不需要合并
			if(ans.size() == 0 || preVal < cur[0]){
				ans.add(cur);
				// 更新preVal
				preVal = ans.get(ans.size() - 1)[1];
			}else{
				// 如果可以合并, 更新preVal和ans
				preVal = Math.max(preVal, cur[1]);
				ans.get(ans.size() - 1)[1] = preVal;
			}
		}
		return ans.toArray(new int[ans.size()][]);
	}


	/**
	 * 换个写法
	 * @param intervals
	 * @return
	 */
	public static int[][] merge2(int[][] intervals){

		if(intervals.length < 1){
			return intervals;
		}
		Arrays.sort(intervals, Comparator.comparingInt(v -> v[0]));
		List<int[]> ans = new ArrayList<>();
		// 初始值
		ans.add(intervals[0]);
		int preIntervalEndValue = intervals[0][1];

		for(int i = 1; i < intervals.length; i++){
			int[] cur = intervals[i];
			// 如果不需要合并, 则将cur加到ans中
			if(preIntervalEndValue < cur[0]){
				preIntervalEndValue = cur[1];
				ans.add(cur);
			}else{
				// 如果可以合并, 则更新ans最新一个interval的end值
				preIntervalEndValue = Math.max(preIntervalEndValue, cur[1]);
				ans.get(ans.size() - 1)[1] = preIntervalEndValue;
			}
		}
		return ans.toArray(new int[ans.size()][]);
	}

}
