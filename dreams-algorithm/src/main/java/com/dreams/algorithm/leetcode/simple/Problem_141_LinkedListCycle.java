package com.dreams.algorithm.leetcode.simple;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/10/9 10:38
 * @description TODO
 */
public class Problem_141_LinkedListCycle {

	public static void main(String[] args) {

	}

	/**
	 * 判断链表是否有环
	 * @param head
	 * @return
	 */
	public boolean hasCycle(ListNode head){
		if(head == null || head.next == null){
			return false;
		}
		ListNode fast = head.next;
		ListNode slow = head;

		while (slow != fast){
			// 快慢指针结束
			if(fast == null || slow == null){
				return false;
			}
			slow = slow.next;
			fast = fast.next.next;
		}
		return true;
	}


}

class ListNode{
	int val;
	ListNode next;
	ListNode(int x){
		val = x;
		next = null;
	}
}
