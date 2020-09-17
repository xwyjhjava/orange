package com.dreams.algorithm.leetcode.medium;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/9/11 14:34
 * @description Given two integers n and k, return all possible combinations of k numbers out of 1 ... n.
 *
 */
public class Combinations extends AbstractSolution{

	@Override
	void solute() {
		super.solute();

//		combineFailed(4, 3); //  4,3 时算法不满足
	}

	/**
	 * 时间窗口失败的尝试, 保留记录
	 * @param n
	 * @param k
	 * @return
	 */
	public List<List<Integer>> combineFailed(int n, int k) {

		// k 代表初始窗口大小
		List<List<Integer>> resultList = new ArrayList<>();
		// 边界值处理
		if(k == 0){
			return resultList;
		}
		if(k == 1){
			for (int i = 0; i < n; i++) {
				List<Integer> innerList = new ArrayList<>();
				innerList.add(i + 1);
				resultList.add(innerList);
			}
			return resultList;
		}
		int flag = k;
		for (int i = 0; i < n; i++) {
			// 窗口内的值符合要求
			// 循环 i+1 后面的元素, 窗口值向右滑动
			for (int j = i + 1; j < n; j++) {
				List<Integer> innerList = new ArrayList<>();
				innerList.add(i + 1);
				int tmp = j;
				while(flag - 1 != 0){
					if(tmp + 1 > n){
						break;
					}
					innerList.add(tmp + 1);
					tmp++;
					flag--;
				}
				// 重置 k 值
				flag = k;
				if(innerList.size() == k){
					resultList.add(innerList);
				}

			}
		}

		return resultList;
	}


}
