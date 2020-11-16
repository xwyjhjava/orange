package com.dreams.algorithm.leetcode.medium;

import java.util.*;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/11/15 21:59
 * @description TODO 待解决
 */
public class Problem_402_RemoveKDigits {

	public static void main(String[] args) {
		String num = "5587";
		int k = 1000;
		String ans = removeKdigits(num, k);
		System.out.println("ans = " + ans);
	}

	/**
	 * Given a non-negative integer num represented as a string,
	 * remove k digits from the number so that the new number is
	 * the smallest possible.
	 *
	 * Note:
	 * The length of num is less than 10002 and will be ≥ k.
	 * The given num does not contain any leading zero.
	 *
	 * 来源：力扣（LeetCode）
	 * 链接：https://leetcode-cn.com/problems/remove-k-digits
	 * 著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
	 *
	 * 解法： 单调栈, 这个解法不简洁, 效率低
	 * @param num
	 * @param k
	 * @return
	 */
	public static String removeKdigits(String num, int k) {

		if(k >= num.length()){
			return "0";
		}
		// 移掉k个数保证剩下的最小
		// 尽可能移掉较大的数, 被移除的标记为-1
		Stack<Character> stack = new Stack<>();
		char[] str = num.toCharArray();
		stack.push(str[0]);

		for(int index = 1; index < str.length; index++){
			// 边界是: num=5337, k=2,  stack第一次判断就会pop
			char top = stack.isEmpty() == true ? '0' : stack.peek();
			// 如果遍历到当前元素比栈顶大， 则压栈
			// 否则, 则弹出, 继续比较
			if(top <= str[index]){
				stack.push(str[index]);
				// 边界是： 直到遍历到最后一个元素, k都不为0
				if(index == str.length - 1 && k > 0){
					while(k > 0){
						stack.pop();
						k--;
					}
				}
			}else{
				//弹出
				stack.pop();
				k--;
				index--;
			}
			// 边界是: k提前为0, str未遍历完, 剩下的数依次压栈
			if(k == 0){
				index++;
				// 剩下的数依次进栈
				while(index < str.length){
					stack.push(str[index]);
					index++;
				}
				break;
			}
		}
		StringBuilder builder = new StringBuilder();
		while(!stack.isEmpty()){
			builder.append(stack.pop());
		}
		// 去掉前置0
		String ans = builder.reverse().toString();
		// 边界: ans=107,  处理出结果为7, 不正确
		while (ans.startsWith("0") && ans.length() > 1){
			//边界: 反转后是0000开头的数
			ans = ans.substring(ans.indexOf('0') + 1);
		}
		return ans;
	}

	/**
	 * 官方实现
	 * @return
	 */
	public static String removeKdigits2(String num, int k){
		// 双端队列, 便于后续不用将字符串反转
		Deque<Character> deque = new LinkedList<Character>();
		int length = num.length();
		// 遍历num
		for (int i = 0; i < length; ++i) {
			// 当前字符
			char digit = num.charAt(i);
			// 队列需要pop元素的条件:   (1)队列不为空 (2) k > 0 (3) 队列顶值 > 当前值
			// 周而复始
			while (!deque.isEmpty() && k > 0 && deque.peekLast() > digit) {
				deque.pollLast();
				// 更新 k
				k--;
			}
			// 当前字符压栈
			deque.offerLast(digit);
		}

		// 遍历完字符串， k > 0, 依次pop队列顶值
		for (int i = 0; i < k; ++i) {
			deque.pollLast();
		}

		StringBuilder ret = new StringBuilder();
		// 处理前置0
		// 标记是否是0开头
		boolean leadingZero = true;
		while (!deque.isEmpty()) {
			char digit = deque.pollFirst();
			if (leadingZero && digit == '0') {
				continue;
			}
			leadingZero = false;
			ret.append(digit);
		}
		return ret.length() == 0 ? "0" : ret.toString();
	}
}
