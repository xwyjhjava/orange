package com.dreams.algorithm.leetcode.medium;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/11/21 11:04
 * @description TODO
 */
public class Problem_148_SortList {

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
	 * 链表排序, 要求时间复杂度O(NlogN), 额外空间复杂度为O(1)
	 * @param head  The number of nodes in the list is in the range [0, 5 * 104].
	 *              -10^5 <= Node.val <= 10^5
	 * @return
	 */
	public ListNode sortList(ListNode head) {
		// 链表排序
		if(head == null || head.next == null){
			return head;
		}

		return head;
	}
}
