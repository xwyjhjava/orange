package com.dreams.algorithm.leetcode.medium;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/9/30 12:14
 * @description TODO
 */
// TODO : 待完成
public class Problem_701_InsertBinarySearchTree {

	public static void main(String[] args) {

	}

	/**
	 * 给定一棵BST(Binary Search Tree), 插入一个值，构成新的BST
	 * @param root
	 * @param val
	 * @return
	 */
	public TreeNode insertIntoBST(TreeNode root, int val) {
		if(root == null){
			return new TreeNode(val, null, null);
		}
		// 比较 val 和 root.val 的大小关系

		// 插入值比头结点小
		if(root.val > val){
			// 继续在左子树找

		}

		return null;
	}

	public TreeNode findNode(int val, TreeNode node){

		return new TreeNode(val, null, null);
	}

}