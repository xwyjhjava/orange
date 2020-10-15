package com.dreams.algorithm.leetcode.medium;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/10/10 10:11
 * @description TODO
 */
public class Problem_142_LinkedListCycle2 {

	public static void main(String[] args) {

	}

	public static class ListNode{
		int val;
		ListNode next;
		ListNode(int x){
			val = x;
			next = null;
		}
	}

	/**
	 * 哈希表解法
	 * 额外空间复杂度: O(N)
	 * @param head
	 * @return
	 */
	public ListNode detectCycle(ListNode head){
		if(head == null || head.next == null){
			return null;
		}
		ListNode slow = head;
		Set<ListNode> vistedSet = new HashSet<>();
		while(slow != null){
			if(vistedSet.contains(slow)){
				return slow;
			}
			vistedSet.add(slow);
			slow = slow.next;
		}
		return null;
	}


	/**
	 * 快慢指针
	 * @param head
	 * @return
	 */
	public ListNode detectCycle_2(ListNode head){
		if(head == null || head.next == null){
			return null;
		}
		ListNode fast = head.next;
		ListNode slow = head;
		// 当fast和slow相遇时， while循环结束
		while(fast != slow){
			if(fast == null || fast.next == null){
				return null;
			}
			fast = fast.next.next;
			slow = slow.next;
		}
		fast = head;
		// 根据数学推导的结论： 此时慢指针和头指针同时走，相遇的点即为cycle的入口
		while (fast != slow){
			fast = fast.next;
			slow = slow.next;
		}
		return fast;
	}





}

