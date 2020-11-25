package com.dreams.algorithm.leetcode.medium;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/11/24 10:50
 * @description TODO
 */
public class Problem_222_CountCompleteTreeNodes {


	public static void main(String[] args) {

	}

	/**
	 * Given a complete binary tree, count the number of nodes.
	 * @param root
	 * @return
	 */
	public int countNodes(TreeNode root){

		// 完全二叉树, 计算节点个数, 可以BFS或者DFS
		return dfs(root);
	}


	public int dfs(TreeNode treeNode){
		if(treeNode == null){
			return 0;
		}
		return dfs(treeNode.left) + dfs(treeNode.right) + 1;
	}


	// =======================解法2==============================
	public int countNodes2(TreeNode root){
		// BFS
		if(root == null){
			return 0;
		}
		Queue<TreeNode> queue = new LinkedList<>();
		queue.add(root);
		int ans = 0;
		while(!queue.isEmpty()){
			// 当前节点
			TreeNode cur = queue.poll();
			ans++;
			if(cur.left != null){
				queue.add(cur.left);
			}
			if(cur.right != null){
				queue.add(cur.right);
			}
		}
		return ans;
	}

	// =====================解法3================================
	public int countNodes3(TreeNode root){
		//morris
		if(root == null){
			return 0;
		}
		TreeNode cur = root;
		TreeNode mostRight;
		int ans = 0;
		while(cur != null){
			ans++;
			// 如果cur无左树, 则cur = cur.right
			// 否则cur有左树
			if(cur.left != null){
				// 找到左树的最右节点 mostRight
				mostRight = cur.left;
				while(mostRight.right != null && mostRight.right != cur){
					mostRight = mostRight.right;
				}
				// 判断最右节点的右指针
				if(mostRight.right == null){
					// 如果mostRight的右指针指向null
					mostRight.right = cur;
					cur = cur.left;
				}else{
					// 如果mostRight的右指针指向cur
					mostRight.right = null;
					cur = cur.right;
					// 此时第二次来到节点，不计数
					ans--;
				}
			}else{
				cur = cur.right;
			}
		}

		return ans;


	}


}
