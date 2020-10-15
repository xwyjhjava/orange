package com.dreams.algorithm.linkedlist;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.linkedlist
 * @date 2020/10/10 11:48
 * @description TODO
 */
public class LinkedListCore {


	/**
	 * 双向链表实现栈和队列
	 * @param <T>
	 */
	public static class DoubleEndLinkedList<T>{
		Node<T> head;
		Node<T> tail;

		// add from head方法
		public void addDataFromHead(T value){
			Node<T> cur = new Node<>(value);
			// 头节点为空时, 表示此时是第一个进来的数，头尾指针都指向该值
			if(head == null){
				head = cur;
				tail = cur;
			}else{ // 继续在头部加入值
				// 新加入的值变成头部
				cur.next = head;
				head.pre = cur;
				head = cur;
			}
		}

		// add from bottom
		public void addDataFromBottom(T value){
			Node<T> cur = new Node<>(value);
			// 头节点为空时, 表示此时是第一个进来的数，头尾指针都指向该值
			if(head == null){
				head = cur;
				tail = cur;
			}else{ // 继续在尾部加入值
				// 新加入的值变成尾部
				cur.pre = tail;
				tail.next = cur;
				tail = cur;
			}
		}


		// pop from head
		public T popFromHead(){
			if(head == null){
				return null;
			}
			Node<T> cur = head;
			if(head == tail){ // 只有一个节点
				head = null;
				tail = null;
			}else {
				// 头指针向后移
				head = cur.next;
				// 清空当前节点的前后指针(从链表中断开)
				cur.next = null;
				head.pre = null;
			}
			return cur.value;
		}

		// pop from bottom
		public T popFromBottom(){
			if(head == null){
				return null;
			}
			Node<T> cur = tail;
			if(head == tail){ // 只有一个节点
				head = null;
				tail = null;
			}else {
				// 尾指针向前移
				tail = cur.pre;
				// 清空当前节点的前后指针(从链表中断开)
				tail.next = null;
				cur.pre = null;
			}
			return cur.value;
		}


		/**
		 * 栈: 先进后出
		 * @param <T>
		 */
		public static class MyStack<T>{
			private DoubleEndLinkedList stack = new DoubleEndLinkedList();

			public void push(T value){
				stack.addDataFromHead(value);
			}

			public void pop(){
				stack.popFromHead();
			}
		}

		/**
		 * 队列： 先进先出
		 * @param <T>
		 */
		public static class MyQueue<T>{
			private DoubleEndLinkedList queue = new DoubleEndLinkedList();

			public void push(T value){
				queue.addDataFromHead(value);
			}

			public void pop(){
				queue.popFromBottom();
			}
		}


	}





}

class Node<T>{
	T value;
	Node pre;
	Node next;

	Node(T data){
		value = data;
	}
}
