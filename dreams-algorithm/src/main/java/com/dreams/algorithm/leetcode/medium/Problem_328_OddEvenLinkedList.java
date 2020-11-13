package com.dreams.algorithm.leetcode.medium;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/11/13 14:41
 * @description TODO
 */
public class Problem_328_OddEvenLinkedList {


	public static class ListNode{
		int val;
        ListNode next;
        ListNode() {}
        ListNode(int val) { this.val = val; }
        ListNode(int val, ListNode next) { this.val = val; this.next = next; }
	}

	public static void main(String[] args) {

	}


	/**
	 * Given a singly linked list, group all odd nodes together followed by the even nodes.
	 * Please note here we are talking about the node number and not the value in the nodes.
	 *
	 * You should try to do it in place. The program should run in O(1) space complexity and O(nodes) time complexity.
	 *
	 * @param head
	 * @return
	 */
	public static ListNode oddEvenList(ListNode head){
		if(head == null || head.next == null){
			return head;
		}

		/**
		 * 步骤： 例如 1->2->3->null
		 *      (1). 节点1指向节点3， odd来到节点3的位置
		 *      (2). 节点2指向节点3的后继null, 用一个指针evenHead记住节点2, even来到null
		 *      (3). 遍历结束后, odd指向evenHead
		 *
		 */
		// 奇数节点
		ListNode odd = head;
		// 偶数节点
		ListNode even = head.next;
		// 偶数节点的头
		ListNode evenHead = even;

		while(odd.next != null && even.next != null){
			// 节点1指向节点3
			odd.next = even.next;
			// odd来到节点3的位置
			odd = even.next;
			// 节点2指向节点3的后继
			even.next = odd.next;
			// 用一个指针记住节点2, 初始化已完成
			// even 来到节点3的后继
			even = odd.next;
		}
		// 遍历结束后
		odd.next = evenHead;
		return head;
	}



}
