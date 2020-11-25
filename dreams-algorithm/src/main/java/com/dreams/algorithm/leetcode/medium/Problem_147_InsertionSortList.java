package com.dreams.algorithm.leetcode.medium;


import com.dreams.algorithm.leetcode.medium.Problem_147_InsertionSortList.ListNode;
/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/11/20 10:45
 * @description TODO
 */
public class Problem_147_InsertionSortList {

	public static class ListNode{
		int val;
		ListNode next;

		public ListNode(int val) {
			this.val = val;
		}
	}

	public static void main(String[] args) {
		ListNode head = new ListNode(1);
		ListNode first = new ListNode(2);
		ListNode second = new ListNode(3);
		ListNode third = new ListNode(4);

		third.next = null;
		second.next = third;
		first.next = second;
		head.next = first;

		insertionSortList(head);

	}

	/**
	 * Sort a linked list using insertion sort.
	 *
	 * @param head
	 * @return
	 */
	public static ListNode insertionSortList(ListNode head){
		if(head == null || head.next == null){
			return head;
		}
		// 虚拟头节点
		ListNode dummyHead = new ListNode(0);
		dummyHead.next = head;
		// 待排序的节点
		ListNode curr = head.next;
		// 有序的最后一个节点
		ListNode lastSorted = head;
		while(curr != null){
			// 比较两个节点的值
			// 假如是降序，则此时需要交换两个节点
			// 否则lastSorted 和 cur 就往后移动
			if(curr.val < lastSorted.val){
				ListNode pre = dummyHead;
				// 因为能找到一个需要待插入的数, 所以这个while循环必然能在合理的范围内停住，不会越界
				while(pre.next.val <= curr.val){
					pre = pre.next;
				}
				// 0 -> 1 -> 7 -> 3 -> null
				// 此时 lastSorted 在 7 的位置, curr在 3 的位置,  pre在 1 的位置
				//
				// 第一步:  7 -> null
				lastSorted.next = curr.next;
				// 第二步:  3 -> 7
				curr.next = pre.next;
				// 第三步:  1 -> 3
				pre.next = curr;
			}else{
				lastSorted = lastSorted.next;
			}
			curr = lastSorted.next;
		}
		return dummyHead.next;
	}


}
