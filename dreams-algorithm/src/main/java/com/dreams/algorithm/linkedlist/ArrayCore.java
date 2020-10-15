package com.dreams.algorithm.linkedlist;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.linkedlist
 * @date 2020/10/10 15:07
 * @description TODO
 */
public class ArrayCore {

	public static class MyStack{
		int[] arr;
		int index;
		int limit;


		public MyStack(int len) {
			limit = len;
			index = 0;
			arr = new int[limit];
		}

		/**
		 * 添加元素
		 * @param value
		 */
		public void push(int value){
			if(index == limit){
				throw new RuntimeException("stack is full");
			}
			arr[index] = value;
			index ++;
		}

		/**
		 * 弹出元素
		 * @return
		 */
		public int pop(){
			if(index == 0){
				throw new RuntimeException("stack is empty");
			}
			// 拿index - 1位置的数
			int result = arr[index - 1];
			index --;
			return result;
		}
	}

	/**
	 * 环形队列
	 */
	public static class MyQueue{
		int[] arr;
		int putIndex;
		int popIndex;
		// 描述当前队列中元素的个数
		int size;
		final int limit;

		public MyQueue(int limit) {
			arr = new int[limit];
			putIndex = 0;
			popIndex = 0;
			size = 0;
			this.limit = limit;
		}

		/**
		 * 添加数据
		 * @param value
		 */
		public void push(int value){
			if(size == limit){
				throw new RuntimeException("queue is full");
			}
			arr[putIndex] = value;
			size ++;
			putIndex = putIndex < limit - 1 ? putIndex + 1 : 0;
		}

		/**
		 * 弹出数据
		 * @return
		 */
		public int pop(){
			if(size == 0){
				throw new RuntimeException("queue is empty");
			}
			int result = arr[popIndex];
			size --;
			popIndex = popIndex < limit - 1 ? popIndex + 1 : 0;
			return result;
		}


	}


}
