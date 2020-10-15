package com.dreams.algorithm.linkedlist;

import java.util.Stack;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.linkedlist
 * @date 2020/10/10 15:51
 * @description TODO
 */
public class StackCore {

	public static void main(String[] args) {

	}

	/**
	 * 实现一个栈， get, put, getMin三个方法， 要求时间复杂度都为O(1)
	 */
	public static class myStack{
		Stack<Integer> dataStack;
		Stack<Integer> minStack;

		public myStack() {
			this.dataStack = new Stack<>();
			this.minStack = new Stack<>();
		}

		/**
		 * 添加元素
		 * @param value
		 */
		public void push(int value){
			// min 栈为空时
			if(minStack.isEmpty()){
				minStack.push(value);
			}else{ // min 栈不为空时，当前元素和min栈栈顶元素比较
				Integer min = minStack.peek();
				if(min <= value){
					minStack.push(min);
				}else{
					minStack.push(value);
				}
			}
			dataStack.push(value);
		}

		/**
		 * 弹出元素
		 * @return
		 */
		public int pop(){
			if(dataStack.isEmpty()){
				return -1;
			}
			minStack.pop();
			int result = dataStack.pop();
			return result;
		}


		public int getMin(){
			if(minStack.isEmpty()){
				return -1;
			}
			return minStack.peek();
		}
	}


	/**
	 * 用栈实现队列
	 */
	public static class TwoStackQueue{
		Stack<Integer> pushStack;
		Stack<Integer> popStack;

		public TwoStackQueue(){
			this.pushStack = new Stack<>();
			this.popStack = new Stack<>();
		}

		/**
		 * pushStack 导数据到popStack, 遵循两条原则
		 *          1. popStack 为空
		 *          2. pushStack 所有数据都导出
		 */
		public void pushToPop(){
			if(popStack.isEmpty()){
				while(!pushStack.isEmpty()){
					popStack.push(pushStack.pop());
				}
			}
		}


		public void add(int value){
			pushStack.push(value);
			pushToPop();
		}

		public int poll(){
			if(popStack.isEmpty() && pushStack.isEmpty()){
				return -1;
			}
			pushToPop();
			return popStack.pop();
		}

		public int peek(){
			if(popStack.isEmpty() && pushStack.isEmpty()){
				return -1;
			}
			pushToPop();
			return popStack.peek();
		}










	}
}
