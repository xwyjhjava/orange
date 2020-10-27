package com.dreams.algorithm.morris;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.morris
 * @date 2020/10/27 14:36
 * @description TODO
 */
public class MorrisCore {

	public static class TreeNode {
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

	/**
	 * morris遍历
	 * @param root
	 */
	public static void morris(TreeNode root){
		if(root == null){
			return;
		}
		TreeNode cur = root;
		TreeNode mostRight;

		while(cur != null){
			// 判断有无左子树
			mostRight = cur.left;
			if(cur.left != null){
				// 找到左子树的最右节点
				while(mostRight.right != null && mostRight.right != cur){
					mostRight = mostRight.right;
				}

				if(mostRight.right == null){
					mostRight.right = cur;
					cur = cur.left;
				}else{  // mostRight.right == cur
					mostRight.right = null;
					cur = cur.right;
				}
			}else{ // 无左子树
				cur = cur.right;
			}
		}
	}


	public static void morris2(TreeNode root){

		if(root == null){
			return;
		}
		TreeNode cur = root;
		TreeNode mostRight;

		while(cur != null){
			// 判断有无左子树
			mostRight = cur.left;
			if(cur.left != null){
				// 找到左子树的最右节点
				while(mostRight.right != null && mostRight.right != cur){
					mostRight = mostRight.right;
				}

				if(mostRight.right == null){
					mostRight.right = cur;
					cur = cur.left;
					continue;
				}else{  // mostRight.right == cur
					mostRight.right = null;
				}
			}
			cur = cur.right;
		}



	}

	/**
	 *
	 * @param root
	 */
	public static void binaryTreeInOrderByMorris(TreeNode root){
		if(root == null){
			return;
		}
		TreeNode cur = root;
		TreeNode mostRight;

		while(cur != null){
			// 判断有无左子树
			mostRight = cur.left;
			if(cur.left != null){
				// 找到左子树的最右节点
				while(mostRight.right != null && mostRight.right != cur){
					mostRight = mostRight.right;
				}

				if(mostRight.right == null){
					mostRight.right = cur;
					cur = cur.left;
					continue;
				}else{  // mostRight.right == cur
					mostRight.right = null;
				}
			}
			System.out.println("cur = " + cur.val);
			cur = cur.right;
		}
	}




	/**
	 * morris 遍历实现二叉树的先序遍历
	 * @param root
	 */
	public static void binaryTreePreorderByMorris(TreeNode root){
		if(root == null){
			return;
		}

		TreeNode cur = root;
		TreeNode mostRight;

		while(cur != null){
			// 判断有无左子树
			mostRight = cur.left;
			if(cur.left != null){
				// 找到左子树的最右节点
				while(mostRight.right != null && mostRight.right != cur){
					mostRight = mostRight.right;
				}

				if(mostRight.right == null){
					mostRight.right = cur;
					System.out.println("cur = " + cur.val);
					cur = cur.left;
				}else{  // mostRight.right == cur
					mostRight.right = null;
					cur = cur.right;
				}
			}else{ // 无左子树
				System.out.println("cur = " + cur.val);
				cur = cur.right;
			}
		}
	}





}

