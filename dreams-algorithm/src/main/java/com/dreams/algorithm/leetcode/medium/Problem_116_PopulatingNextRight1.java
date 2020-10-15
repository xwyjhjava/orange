package com.dreams.algorithm.leetcode.medium;

import java.util.LinkedList;
import java.util.Queue;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/10/15 10:35
 * @description TODO
 */
public class Problem_116_PopulatingNextRight1 {

	public static void main(String[] args) {

		Node left = new Node(2, null, null, null);
		Node right = new Node(3, null, null, null);
		Node root = new Node(1, left, right, null);
		connect(root);
	}


	public static class Node{
		public int val;
		public Node left;
		public Node right;
		public Node next;

		public Node() {
		}

		public Node(int _val) {
			val = _val;
		}

		public Node(int _val, Node _left, Node _right, Node _next) {
			val = _val;
			left = _left;
			right = _right;
			next = _next;
		}
	}

	/**
	 * 时间复杂度为O(N), 空间复杂度O(N)
	 * @param root
	 * @return
	 */
	public static Node connect(Node root){
		if(root == null){
			return null;
		}
		Node cur = root;
		Queue<Node> queue = new LinkedList<>();
		queue.add(root);
		while(!queue.isEmpty()){
			// 当前层的大小
			int size = queue.size();
			for(int i = 0; i < size; i++){
				// 先弹出一个元素cur
				cur = queue.poll();
				// cur的next是队顶元素
				if(i < size - 1){
					cur.next = queue.peek();
				}else{
					cur.next = null;
				}

				if(cur.left != null){
					queue.add(cur.left);
					queue.add(cur.right);
				}
			}
		}
		return root;
	}


	public static Node connect2(Node root){
		if(root == null){
			return null;
		}
		Node left = root;
		// 只要节点有左孩子，说明左右孩子都存在
		while(root.left != null){
			Node head = left;
			while(head != null && head.left != null){

				// 连接同一个head下的左右孩子
				head.left.next = head.right;
				// 连接第一个head的右孩子和第二个head的左孩子, 存在相邻的head
				if(head.next != null){
					head.right.next = head.next.left;
				}
				// 指针向后推进
				head = head.next;
			}
			left = left.left;
		}
		return root;
	}



}
