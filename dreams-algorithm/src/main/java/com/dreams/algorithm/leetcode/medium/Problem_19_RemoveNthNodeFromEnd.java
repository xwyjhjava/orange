package com.dreams.algorithm.leetcode.medium;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/10/19 18:05
 * @description TODO
 */
public class Problem_19_RemoveNthNodeFromEnd {

	public static class ListNode{
		int val;
		ListNode next;
		ListNode(){}

		public ListNode(int val) {
			this.val = val;
		}

		public ListNode(int val, ListNode next) {
			this.val = val;
			this.next = next;
		}
	}


	public static void main(String[] args) {

	}


	/**
	 * Given the head of a linked list, remove the nth node from the end of list
	 * and return its head.
	 *
	 * 1 <= listSize <= 30
	 * 0 <= Node.val <=100
	 * 1 <= n <= listSize
	 *
	 * @param head
	 * @param n
	 * @return
	 */
	public ListNode removeNthFromEnd(ListNode head, int n) {

		// ListNode 只有一个节点， 并且n==1
		if(head.next == null){
			return null;
		}
		// 当前来到的节点
		ListNode cur = head;
		// 前一个节点
		ListNode preNode = head;
		// 待删除的节点
		ListNode deleteNode = head;
		// delete倒数计数
		int lastCount = 1;
		// 判断当前节点是否为空
		while(cur != null){
			if(lastCount < n){
				if(cur.next != null){
					lastCount++;
				}
			}else if(lastCount == n){
				// 表示此时链表结束了
				if(cur.next == null){
					// 如果删除的是头节点
					if(preNode == deleteNode){
						head = preNode.next;
					}else{
						preNode.next = deleteNode.next;
//						head = preNode;
					}
				}else{
					preNode = deleteNode;
					deleteNode = deleteNode.next;
//					lastCount--;
				}
			}
			cur = cur.next;
		}
		return head;
	}
}
