package com.dreams.algorithm.leetcode.medium;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/10/27 14:24
 * @description TODO
 */
public class Problem_144_BinaryTreePreorder {


	public class TreeNode{
		int val;
		TreeNode left;
		TreeNode right;
		TreeNode(){}
		TreeNode(int val){
			this.val = val;
		}
		public TreeNode(int val, TreeNode left, TreeNode right) {
			this.val = val;
			this.left = left;
			this.right = right;
		}
	}

	public static void main(String[] args) {

	}

	static List<Integer> ans = new ArrayList<>();

	/**
	 * Given the root of a binary tree, return the preorder traversal of its nodes' values
	 *
	 * 实现： morris遍历
	 * @return
	 */
	public static List<Integer> preorderTraversal(TreeNode root){
		if(root == null){
			return ans;
		}
		TreeNode cur = root;
		TreeNode mostRight;
		while(cur != null){
			mostRight = cur.left;
			// 有无子树
			if(cur.left != null){

				//找到左子树的最右节点
				while(mostRight.right != null && mostRight.right != cur){
					mostRight = mostRight.right;
				}
				// 第一次到达
				if(mostRight.right == null){
					mostRight.right = cur;
					ans.add(cur.val);
					cur =cur.left;
				}else{
					mostRight.right = null;
					cur = cur.right;
				}
			}else{
				ans.add(cur.val);
				cur = cur.right;
			}
		}
		return ans;
	}
}
