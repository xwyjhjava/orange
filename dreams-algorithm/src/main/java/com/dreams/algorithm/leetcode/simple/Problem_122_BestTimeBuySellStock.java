package com.dreams.algorithm.leetcode.simple;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/11/8 16:03
 * @description TODO
 */
public class Problem_122_BestTimeBuySellStock {

	public static void main(String[] args) {

	}


	/**
	 *
	 * Say you have an array prices for which the ith element is the price of a given stock on day i.
	 * Design an algorithm to find the maximum profit. You may complete as many transactions as you like
	 * (i.e., buy one and sell one share of the stock multiple times).
	 *
	 * Note: You may not engage in multiple transactions at the same time
	 * (i.e., you must sell the stock before you buy again).
	 *
	 * 股票问题
	 * @param prices
	 * @return
	 */
	public static int maxProfit(int[] prices) {
		// 只有一天的股价，无法完成交易
		if(prices.length < 2){
			return 0;
		}
		// 买入价格
		int buyPrice = 0;
		// 卖出价格
		int salePrice = 0;
		int profit = 0;
		boolean isBuyed = false;

		// 前一天价格 < 后一天价格时，尝试买入
		// 在有买入时，前一天价格 > 后一天价格， 尝试卖出， 计算收益
		for(int i = 0; i < prices.length - 1; i++){
			// 前一天价格 < 后一天价格, 如果未买过，尝试买入
			// 否则就是股价在涨，继续持有，不卖出
			if(prices[i] < prices[i + 1]){
				if(!isBuyed){
					buyPrice = prices[i];
					isBuyed = true;
				}
			}else{ // 此时股价在跌, 不买入
				// 如果之前买过了, 就在当天的股价卖出, 计算收益
				if(isBuyed){
					salePrice = prices[i];
					profit += (salePrice - buyPrice);
					isBuyed = false;
				}

			}
		}
		// 如果此时手上还有股票，就在最后一天卖出
		if(isBuyed){
			profit += (prices[prices.length - 1] - buyPrice);
		}
		return profit;
	}
}
