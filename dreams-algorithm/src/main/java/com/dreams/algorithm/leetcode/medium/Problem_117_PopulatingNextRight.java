package com.dreams.algorithm.leetcode.medium;

import java.util.LinkedList;
import java.util.Queue;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/9/28 14:41
 * @description TODO
 */
public class Problem_117_PopulatingNextRight {

	public static void main(String[] args) {

		Node one = new Node();
		Node two = new Node();
		Node three = new Node();
		Node four = new Node();

		four.next = null;
		three.next = null;
		two.next = three;
		one.next = null;

	}

	// 对二叉树做层次遍历,  使用队列实现,  每一次poll的时候，将元素串联起来
	public Node connect(Node root){
		if(root == null){
			return null;
		}
		Queue<Node> queue = new LinkedList<>();
		queue.add(root);
		Node curEnd = root;
		Node nextEnd = null;
		Node result = null;
		Node nextTmp = null;
		while(!queue.isEmpty()){
			Node node = queue.poll();
			if(result == null){
				result = node;
			}
			if(node.left != null){
				queue.add(node.left);
				nextEnd = node.left;
			}
			if(node.right != null){
				queue.add(node.right);
				nextEnd = node.right;
			}
			// 此时表示一行结束
			if(node == curEnd){
				node.next = null;
				curEnd = nextEnd;
			}else{
				result = handle(node, result);
			}
			nextTmp = node.next;




		}
		return result;
	}

	public Node handle(Node p, Node result){

		return null;
	}


	public static Node connect2(Node root){
		if (root == null) {
			return root;
		}
		Queue<Node> queue = new LinkedList<>();
		queue.add(root);
		while (!queue.isEmpty()) {
			//每一层的数量
			int levelCount = queue.size();
			//前一个节点
			Node pre = null;
			for (int i = 0; i < levelCount; i++) {
				//出队
				Node node = queue.poll();
				//如果pre为空就表示node节点是这一行的第一个，
				//没有前一个节点指向他，否则就让前一个节点指向他
				if (pre != null) {
					pre.next = node;
				}
				//然后再让当前节点成为前一个节点
				pre = node;
				//左右子节点如果不为空就入队
				if (node.left != null) {
					queue.add(node.left);
				}
				if (node.right != null) {
					queue.add(node.right);
				}
			}
		}
		return root;
	}



}

class Node {
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
