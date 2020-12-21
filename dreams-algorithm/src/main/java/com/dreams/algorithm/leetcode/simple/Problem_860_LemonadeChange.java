package com.dreams.algorithm.leetcode.simple;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/12/10 15:23
 * @description TODO
 */
public class Problem_860_LemonadeChange {


	public static void main(String[] args) {
//		int[] bills = new int[]{5,5,5,10,20};
//		int[] bills = new int[]{10,10};
//		int[] bills = new int[]{5,5,10,10,20};
		int[] bills = new int[]{5,5,5,10,5,5,10,20,20,20};
//		int[] bills = new int[]{5,5,10,20,5,5,5,5,5,5,5,5,5,10,5,5,20,5,20,5};
		boolean ans = lemonadeChange(bills);
		System.out.println("ans = " + ans);
	}


	/**
	 *
	 * At a lemonade stand, each lemonade costs $5. 
	 *
	 * Customers are standing in a queue to buy from you, and order one at a time (in the order specified by bills).
	 * Each customer will only buy one lemonade and pay with either a $5, $10, or $20 bill.  You must provide the correct
	 * change to each customer, so that the net transaction is that the customer pays $5.
	 *
	 * Note that you don't have any change in hand at first.
	 *
	 * Return true if and only if you can provide every customer with correct change.
	 *
	 * 来源：力扣（LeetCode）
	 * 链接：https://leetcode-cn.com/problems/lemonade-change
	 * 著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
	 *
	 * 解法： 模拟 + 贪心(未优化)
	 * @param bills
	 * @return
	 */
	public static boolean lemonadeChange(int[] bills) {

		if(bills.length < 1){
			return false;
		}
		int five = 0;
		int ten = 0;
		// 如果第一笔交易无法找零，直接return false
		if(bills[0] > 5){
			return false;
		}
		for(int i = 0; i < bills.length; i++){
			if(bills[i] == 5) {
				five++;
			}else if(bills[i] == 10){
				ten++;
			}
			//找零
			int change = bills[i] - 5;
			// 如果找零无法变成0，那么交易失败, return false
			if(change == 0){
				continue;
			}
			while(change >  0){
				// change 一定等于5、15中的一个数
				if(change == 5){
					if(five > 0){
						five--;
						change = change - 5;
					}else{
						return false;
					}
				}
				if(change == 10 || change == 15){

					if(ten > 0){
						ten--;
						change = change - 10;
					}else if(five > 0){
						// 此时 ten == 0
						five--;
						change = change - 5;
					}else{
						return false;
					}
				}

				if(change == 0){
					break;
				}
			}
			// 此时无法找零
			if(change != 0){
				return false;
			}

		}
		return true;
	}


	/**
	 * 模拟 + 贪心(代码优化)
	 * @param bills
	 * @return
	 */
	public static boolean lemonadeChange2(int[] bills){
		int five = 0, ten = 0;
		for (int bill : bills) {
			if (bill == 5) {
				five++;
			} else if (bill == 10) {
				if (five == 0) {
					return false;
				}
				five--;
				ten++;
			} else {
				if (five > 0 && ten > 0) {
					five--;
					ten--;
				} else if (five >= 3) {
					five -= 3;
				} else {
					return false;
				}
			}
		}
		return true;
	}



}
