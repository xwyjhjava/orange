package com.dreams.algorithm.leetcode.medium;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/11/18 17:37
 * @description TODO
 */
public class Problem_134_GasStation {

	public static void main(String[] args) {
		int[] gas = {1,2,3,4,5};
		int[] cost = {3,4,5,1,2};

//		int[] gas = {2,3,4};
//		int[] cost = {3,4,3};

//		int[] gas = {5,1,2,3,4};
//		int[] cost = {4,4,1,5,1};
		int ans = canCompleteCircuit(gas, cost);
		System.out.println("ans = " + ans);
	}

	public static int canCompleteCircuit(int[] gas, int[] cost) {

		// 尝试
		// 假设是从i位置出发, 则油箱中的油=上次剩下的油+i位置的油
		// 如果此时i位置的油量 >= cost[i], 说明是可以到达下一个地点
		// 否则就不能到达

		// 暴力解法
		int ans = -1;
		for(int index = 0; index < gas.length; index++){
			//剩余的油量, 每次验证, 油量都归0
			int remain = 0;
			// 当前验证的位置
			int cur = index;
			int position = index;
			// 验证次数
			int size = gas.length;
			size--;
			while(true){
				// 从gas[0]位置开始验证是否能到达gas[0]
				// 当前点可利用的油
				int curGas = remain + gas[position];
				// 需要付出的代价
				int curCost = cost[position];
				if(curCost > curGas){
					// 此时到不了, 直接验证下一个
					break;
				}else{
					// 更新剩余油量
					remain = curGas - curCost;
					position = position + 1 >= gas.length ? (position + 1) % gas.length: position + 1;
				}
				// 如果此时position回到了cur, 则return
				// 找到唯一解
				if(position == cur){
					return position;
				}
				// 如果整个数组都验证过了, 则说明没有符合条件的解
				if(size == 0){
					return ans;
				}
			}
		}
		// 所有数都验证过一遍，没有找到一个position
		return ans;
	}
}
