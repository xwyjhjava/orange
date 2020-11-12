package com.dreams.algorithm.leetcode.medium;

import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Stack;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/11/9 11:02
 * @description TODO
 */
public class Problem_973_KClosestPoints {

	public static void main(String[] args) {

		PriorityQueue<Long> queue = new PriorityQueue<>(2, (o1, o2) -> {
			if(o1 >= o2){
				return -1;
			}else{
				return 1;
			}
		});

		queue.add(2L);
		queue.add(5L);
		System.out.println("size1 = " + queue.size());
		queue.add(8L);
		System.out.println("size2 = " + queue.size());

		System.out.println("queue = " + queue.poll());

		System.out.println("pow = " + Math.pow(3, 2));

		double pow = Math.pow(3, 2);

		System.out.println("Integer.MAX_VALUE = " + Integer.MAX_VALUE);
		System.out.println(Integer.MAX_VALUE > 10000*10000);


	}

	/**
	 *
	 * We have a list of points on the plane.  Find the K closest points to the origin (0, 0).
	 * (Here, the distance between two points on a plane is the Euclidean distance.)
	 * You may return the answer in any order.
	 * The answer is guaranteed to be unique (except for the order that it is in.)
	 *
	 * 解法： 大根堆
	 * @param points  -10000 < points[i][0] < 10000
	 * @param k    1 <= K <= points.length
	 * @return
	 */
	public static int[][] kClosest(int[][] points, int k){

		if(k >= points.length){
			return points;
		}

		PriorityQueue<int[]> queue = new PriorityQueue<>((o1, o2) -> {
			Integer o1Dis = calculateDistance(o1);
			Integer o2Dis = calculateDistance(o2);
			if(o1Dis > o2Dis){
				return -1;
			}else{
				return 1;
			}
		});

		int index;
		// 从 points 加入k 个到queue中
		for (index = 0; index < k && index < points.length; index++) {
			queue.add(points[index]);
		}

		// points 的坐标数够k个

		for(int i = index; i < points.length; i++){
			// 当前坐标的值
			int curDis = calculateDistance(points[i]);
			// 大根堆的堆顶元素
			Integer topDis = calculateDistance(queue.peek());
			// 当前坐标值比堆顶小时, 弹出堆顶
			if(curDis < topDis){
				queue.poll();
				queue.add(points[i]);
			}
		}
		// 返回此时堆中的坐标元素
		return queue.toArray(new int[k][]);
	}


	public static Integer calculateDistance(int[] point){
		Integer value = point[0] * point[0] + point[1] * point[1];
		return value;
	}
}
