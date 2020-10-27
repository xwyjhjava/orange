package com.dreams.algorithm.leetcode.simple;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/10/23 11:10
 * @description TODO
 */
public class Problem_234_PalindromeLinkedList {

	public static void main(String[] args) {

	}

	public static class ListNode{
		int val;
		ListNode next;

		public ListNode(int val) {
			this.val = val;
		}
	}


	/**
	 * Given a singly linked list, determine if it is a palindrome.
	 * O(n) time and O(1) space
	 *
	 * 算法步骤：
	 *          1. 找到前半部分链表的尾节点
	 *          2. 反转后半部分链表
	 *          3. 判断是否回文
	 *          4. 恢复链表
	 *          5. 返回结果
	 * 快慢指针实现
	 * @param head
	 * @return
	 */
	public boolean isPalindrome(ListNode head){
		if(head == null || head.next == null){
			return false;
		}
		// 快指针
		ListNode fast = head;
		// 慢指针
		ListNode slow = head;

		// 快慢指针找到 mid 节点
		// fast.next != null && fast.next.next != null 的判断在本题中会造成奇偶长度的链表到达的中点有差异
		// 奇数时， slow来到第一个回文段的结束点； 偶数时，slow来到下一个回文段的开始点
		while(fast.next != null && fast.next.next != null){
			// fast 走两步， slow走一步， 当fast到达末尾时， slow来到中间
			fast = fast.next.next;
			slow = slow.next;
		}
		// 此时slow来到的位置就是中间节点
		ListNode mid = slow;

		// 反转后半部分链表
		ListNode reverseNode = reverseList(mid);

		// 比较前后链表的值，判断是否是回文
		// 这里可以复用fast 和 slow 指针
		fast = head;
		slow = reverseNode;
		while(slow.next != null){
			if(fast.val == slow.val){
				fast = fast.next;
				slow = slow.next;
			}else{
				return false;
			}
		}
		return true;
	}


	// 反转链表
	public static ListNode reverseList(ListNode head){
		ListNode pre = null;
		ListNode cur = head;
		ListNode tmp;
		while(cur != null){
			tmp = cur.next;
			cur.next = pre;
			pre = cur;
			cur = tmp;
		}
		return pre;
	}

}
