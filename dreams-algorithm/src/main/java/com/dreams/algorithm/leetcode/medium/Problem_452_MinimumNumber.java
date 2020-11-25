package com.dreams.algorithm.leetcode.medium;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/11/23 10:24
 * @description TODO
 */
public class Problem_452_MinimumNumber {

	public static void main(String[] args) {

	}

	/**
	 * There are some spherical balloons spread in two-dimensional space.
	 * For each balloon, provided input is the start and end coordinates of the
	 * horizontal diameter. Since it's horizontal, y-coordinates don't matter,
	 * and hence the x-coordinates of start and end of the diameter suffice.
	 * The start is always smaller than the end.
	 *
	 * An arrow can be shot up exactly vertically from different points along the
	 * x-axis. A balloon with xstart and xend bursts by an arrow shot at x
	 * if xstart ≤ x ≤ xend. There is no limit to the number of arrows that can
	 * be shot. An arrow once shot keeps traveling up infinitely.
	 *
	 * Given an array points where points[i] = [xstart, xend], return the minimum
	 * number of arrows that must be shot to burst all balloons.
	 *
	 * 来源：力扣（LeetCode）
	 * 链接：https://leetcode-cn.com/problems/minimum-number-of-arrows-to-burst-balloons
	 * 著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
	 *
	 * @param points  0 <= points.length <= 10^4
	 *                points[i].length == 2
	 *                -2^32 <= xstart < xend <= 2^31 - 1
	 * @return
	 */
	public static int findMinArrowShots(int[][] points){
		if(points.length == 0){
			return 0;
		}
		// 想找到最少数量的箭,  针对本题则可能是范围上的尝试或者是贪心

		// 由于本题的数据范围， 加减可能造成int类型溢出
		Arrays.sort(points, new Comparator<int[]>() {
			@Override
			public int compare(int[] o1, int[] o2) {
				if(o1[1] > o2[1]){
					return 1;
				}else if(o1[1] < o2[1]){
					return -1;
				}else{
					return 0;
				}
			}
		});
		// 从第一个的右边界开始贪心, 作为初始值
		int pos = points[0][1];
		int ans = 1;
		for (int[] balloon: points) {
			// 如果当前气球的start <= pos[0][1]
			// 否则表示这一箭已经射爆了最多的气球， ans++， 更新pos为当前气球的end
			if (balloon[0] > pos) {
				pos = balloon[1];
				ans++;
			}
		}
		return ans;
	}




}
