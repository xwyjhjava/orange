package com.dreams.algorithm.leetcode.medium;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/10/13 13:28
 * @description TODO
 */
public class Problem_24_SwapNodes {

	public static void main(String[] args) {

	}

	public static class ListNode{
		int val;
		ListNode next;
		ListNode(){}
		ListNode(int val){this.val = val;}
		ListNode(int val, ListNode next){this.val = val; this.next = next;}


	}

	/**
	 * swap every two adjacent nodes in a linked list
	 * 给定一个链表， 交换其中相邻的两个节点
	 * @param head
	 * @return
	 */
	public ListNode swapPairs(ListNode head) {
		if(head == null || head.next == null){
			return head;
		}
		ListNode second = head.next;
		head.next = swapPairs(second.next);
		second.next = head;
		return second;
	}


}
