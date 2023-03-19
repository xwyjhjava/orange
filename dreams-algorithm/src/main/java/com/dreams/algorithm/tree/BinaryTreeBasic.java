package com.dreams.algorithm.tree;


import java.util.*;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.tree
 * @date 2020/9/27 15:03
 * @description TODO
 */
public class BinaryTreeBasic {

	public static void main(String[] args) {
		Queue<String> queue = new LinkedList<>();
		queue.addAll(Arrays.asList("1", "2", "4", null, "6", "7", null, null, "8", null, null, null, "3", null, "5", null, null));
		BinaryTreeBasic treeBasic = new BinaryTreeBasic();
		TreeNode treeNode = treeBasic.buildByPreQueue(queue);
		System.out.println("build tree node succes");
		treeBasic.middle(treeNode);
	}



	public void f(TreeNode node){
		if(node == null){
			return;
		}
		f(node.left);
		f(node.right);
	}


	/** 先序遍历  头左右*/
	public void pre(TreeNode node){
		if(node == null){
			return;
		}
		System.out.println(node.value + " ");
		pre(node.left);
		pre(node.right);
	}

	/** 非递归的方式实现先序 */
	/**
	 * 规则： 构建一个stack
	 *       (1) 弹出打印
	 *       (2) 如有右, 压入右
	 *       (3) 如有左, 压入左
	 */
	public void preWithNoRecursion(TreeNode node){
		if(node == null){
			return;
		}
		Stack<TreeNode> stack = new Stack<>();
		// 头节点加入stack
		stack.add(node);
		while (!stack.isEmpty()){
			node = stack.pop();
			System.out.println("stack = " + node.value);
			if(node.right != null){
				stack.push(node.right);
			}
			if(node.left != null){
				stack.push(node.left);
			}
		}
		System.out.println();
	}



	/** 中序遍历  左头右*/
	public void middle(TreeNode node){
		if(node == null){
			return;
		}
		middle(node.left);
		System.out.println(node.value + " ");
		middle(node.right);
	}

	/**
	 * 中序遍历的非递归实现
	 * @param node
	 */
	public void middleWithNoRecursion(TreeNode node){
		if(node == null){
			return;
		}
		Stack<TreeNode> stack = new Stack<>();
		while (!stack.isEmpty() || node != null){
			if(node != null){
				stack.push(node.left);
				node = node.left;
			}else{
				node = stack.pop();
				System.out.println(node.value + " ");
				node = node.right;
			}
		}

	}



	/** 后续遍历   左右头*/
	public void suf(TreeNode node){
		if(node == null){
			return;
		}
		suf(node.left);
		suf(node.right);
		System.out.println(node.value + " ");
	}

	/**
	 * 非递归方式实现后续遍历 (两个stack)
	 * @param node
	 */
	public void sufWithNoRecursion_v1(TreeNode node){
		if(node == null){
			return;
		}
		Stack<TreeNode> s1 = new Stack<>();
		Stack<TreeNode> s2 = new Stack<>();
		s1.add(node);
		while (!s1.isEmpty()){
			node = s1.pop();
			s2.push(node);
			if(node.left != null){
				s1.push(node.left);
			}
			if(node.right != null){
				s1.push(node.right);
			}
		}
		while (!s2.isEmpty()){
			System.out.println(" "+ s2.pop().value);
		}
	}

	/**
	 * 非递归方式实现后续遍历(一个stack)
	 * @param h
	 */
	public void sufWithNoRecursion_v2(TreeNode h){
		if(h == null){
			return;
		}
		// 跟踪上一次pop的节点
		TreeNode previous = h;
		Stack<TreeNode> stack = new Stack<>();
		// 头节点入stack
		stack.push(h);
		while (!stack.isEmpty()){
			// 跟踪到peek节点
			h = stack.peek();
			// 左字树未处理
			if(h.left != null && previous != h.left && previous != h.right){
				// 元素压栈
				stack.push(h.left);
			// 右子树未处理
			}else if(h.right != null && previous != h.right){
				stack.push(h.right);
			// 左右子树都处理完
			}else{
				System.out.println(stack.pop().value + " ");
				previous = h;
			}
		}

	}


	/**
	 * 二叉树按层遍历(队列实现) BFS
	 * @param node
	 */
	public void binarySearchByFloor(TreeNode node){
		if(node != null){
			return;
		}
		Queue<TreeNode> queue = new LinkedList<>();
		queue.add(node);
		while (!queue.isEmpty()){
			node = queue.poll();
			System.out.println(node.value + " ");
			if(node.left != null){
				queue.add(node.left);
			}
			if(node.right != null){
				queue.add(node.right);
			}
		}
	}


	/**
	 * 求二叉树的最大宽度
	 * @param node
	 */
	public void treeMaxWidthUseMap(TreeNode node){
		if(node == null){
			return;
		}
		Queue<TreeNode> queue = new LinkedList<>();

		// 标记元素对应的层数
		HashMap<TreeNode, Integer> curLevelMap = new HashMap<>();
		// 当前层(现在统计的是哪一层的宽度)
		int curLevel = 1;
		curLevelMap.put(node, curLevel);
		// 当前层的节点数
		int nodeNum = 0;
		// 最大层数
		int maxLevel = 0;
		queue.add(node);
		while(!queue.isEmpty()){
			node = queue.poll();
			int curNodeLevel = curLevelMap.get(node);
			if(node.left != null){
				queue.add(node.left);
				curLevelMap.put(node.left, curNodeLevel + 1);
			}
			if(node.right != null){
				queue.add(node.right);
				curLevelMap.put(node.right, curNodeLevel + 1);
			}

			// 当前层面和节点层数一致时，累加节点数
			if(curLevel == curNodeLevel){
				nodeNum++;
			// 否则表示新一层的开始
			}else{
				maxLevel = Math.max(maxLevel, nodeNum);
				curLevel++;
				nodeNum = 1;
			}
		}
		// 最后一层
		maxLevel = Math.max(maxLevel, nodeNum);

		System.out.println("maxLevel = " + maxLevel);
	}


	/**
	 * 求二叉树的最大宽度，不使用map
	 * @param node
	 */
	public void treeMaxWidthNoMap(TreeNode node){
		if(node == null){
			return;
		}
		Queue<TreeNode> queue = new LinkedList<>();
		queue.add(node);
		// 标记当前层的结束
		TreeNode curEnd = node;
		// 标记下一层的结束
		TreeNode nextEnd = null;
		// 层的节点数
		int nodeNum = 0;
		int max = 0;
		while (!queue.isEmpty()){
			node = queue.poll();
			if(node.left != null){
				queue.add(node.left);
				nextEnd = node.left;
			}
			if(node.right != null){
				queue.add(node.right);
				nextEnd = node.right;
			}
			nodeNum++;
			// 当前节点到了最后
			if(node == curEnd){
				max = Math.max(max, nodeNum);
				nodeNum = 0;
				// 更新curEnd, 此时来到了下一层
				curEnd = nextEnd;
			}
		}

	}


	/**
	 * 二叉树序列化
	 * @param node
	 */
	public void preSerial(TreeNode node){
		Queue<String> queue = new LinkedList<>();
		pres(node, queue);
	}
	public void pres(TreeNode node, Queue<String> queue){
		if(node == null){
			queue.add(null);
		}else{
			queue.add(String.valueOf(node.value));
			pres(node.left, queue);
			pres(node.right, queue);
		}
	}


	/**
	 * 二叉树的反序列化
	 * @param prelist
	 */
	public TreeNode buildByPreQueue(Queue<String> prelist){
		if(prelist == null || prelist.size() == 0){
			return null;
		}
		return preb(prelist);
	}
	public TreeNode preb(Queue<String> prelist){
		String value = prelist.poll();
		if(value == null){
			return null;
		}
		TreeNode node = new TreeNode();
		node.value = Integer.valueOf(value);
		node.left = preb(prelist);
		node.right = preb(prelist);
		return node;
	}


	/**
	 *  给定二叉树的某个节点，返回该节点的后继节点
	 *  二叉树结构如下：
	 *          class Node{
	 *              V value;
	 *              Node left;
	 *              Node right;
	 *              Node parent;
	 *          }
	 *  后继节点定义： 中序遍历，节点的相对下一个
	 *  要求时间复杂度为O(K), k为给定的节点和后继节点之间的距离(步长)
	 */
	public NodeWithParent findSuccessor(NodeWithParent node){

		if(node == null) {
			return null;
		}
		// 若给定节点有右子树时， 根据中序遍历的要求， 此时后继节点一定在右子树上
		if(node.right != null){
			return getLeftMost(node.right);
		// 给定节点没有右子树时， 就向上找父节点，直到满足关系 node = node.parent.left
		}else{
			NodeWithParent parent = node.parent;
			// 给定节点是父节点的右孩子时， 继续向上找
			while (parent != null && parent.right == node){
				// node节点向上移
				node = parent;
				parent = node.parent;
			}
			return parent;
		}
	}

	/**
	 * 遍历子树，找到最左的节点
	 * @param rightNode
	 * @return
	 */
	public NodeWithParent getLeftMost(NodeWithParent rightNode){
		if(rightNode.left != null){
			getLeftMost(rightNode.left);
		}
		return rightNode;
	}









}

//  常规二叉树
class TreeNode{
	int value;
	TreeNode left;
	TreeNode right;
}

class NodeWithParent{
	Object value;
	NodeWithParent left;
	NodeWithParent right;
	NodeWithParent parent;
}