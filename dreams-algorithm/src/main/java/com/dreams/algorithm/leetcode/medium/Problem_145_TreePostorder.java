package com.dreams.algorithm.leetcode.medium;

import sun.reflect.generics.tree.Tree;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/9/29 11:19
 * @description 二叉树的后序遍历(左右头)
 */
public class Problem_145_TreePostorder {

	/**
	 * 递归实现
	 * @param root
	 * @return
	 */

	List<Integer> resultList = new ArrayList<>();
	public List<Integer> postorderTraversal(TreeNode root) {
		if(root == null){
			return resultList;
		}
		postorderTraversal(root.left);
		postorderTraversal(root.right);
		resultList.add(root.val);
		return resultList;
	}


	public List<Integer> postorderTraversalNoRecur(TreeNode root) {
		if(root == null){
			return resultList;
		}
		Stack<TreeNode> stack = new Stack<>();
		// 标记上次pop的节点
		TreeNode pre = root;
		stack.push(root);

		while(!stack.isEmpty()){
			// 获取栈顶元素作为当前元素
			TreeNode cur = stack.peek();
			// 左孩子未处理时，继续向下找
			if(cur != null && pre != cur.left){
				stack.push(cur.left);
			}else if(cur != null && pre != cur.right){
				stack.push(cur.right);
			}else {
				resultList.add(stack.pop().val);
				// 更新pre
				pre = cur;
			}
		}
		return resultList;
	}


	/**
	 *
	 * @param root
	 * @return
	 */
	public List<Integer> postorderTraversalNoRecur_v2(TreeNode root){
		List<Integer> resultList = new ArrayList<>();
		if(root == null){
			return resultList;
		}
		Stack<TreeNode> stack = new Stack<>();
		stack.push(root);

		while(!stack.isEmpty()){
			TreeNode cur = stack.pop();
			if(cur != null){
				stack.push(cur);
				stack.push(null);
				if (cur.right != null) {
					stack.push(cur.right);
				}
				if (cur.left != null) {
					stack.push(cur.left);
				}

			}else{
				TreeNode top = stack.pop();
				resultList.add(top.val);
			}
		}
		return resultList;
	}
}


