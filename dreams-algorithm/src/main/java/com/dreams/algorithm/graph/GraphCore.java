package com.dreams.algorithm.graph;

import java.util.*;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.graph
 * @date 2020/10/20 14:29
 * @description TODO
 */
public class GraphCore {

	public static void main(String[] args) {

	}


	/**
	 * 图的广度优先遍历
	 * @param node
	 */
	public static void bfs(Node node){
		if(node == null){
			return;
		}
		Queue<Node> queue = new LinkedList<>();
		queue.add(node);
		Set<Node> nodeSet = new HashSet<>();
		nodeSet.add(node);
		while(!queue.isEmpty()){
			Node cur = queue.poll();
			System.out.println("cur = " + cur.value);
			for(Node next: cur.nexts){
				if(!nodeSet.contains(cur)){
					nodeSet.add(cur);
					queue.add(next);
				}
			}
		}
	}

	/**
	 * 图的深度优先遍历
	 * @param node
	 */
	public static void dfs(Node node){
		if(node == null){
			return;
		}
		Stack<Node> nodeStack = new Stack<>();
		Set<Node> nodeSet = new HashSet<>();
		nodeStack.add(node);
		nodeSet.add(node);
		System.out.println("node.value  == " + node.value);
		while(!nodeStack.isEmpty()){
			// 弹出当前节点
			Node cur = nodeStack.pop();
			for(Node next: cur.nexts){
				if(!nodeSet.contains(next)){
					// 将 cur和 cur.next都压入栈中
					nodeStack.push(cur);
					nodeStack.push(next);
					// 节点注册到set中
					nodeSet.add(next);
					System.out.println("next = " + next.value);
					break;
				}
			}
		}
	}


	/**
	 * 图的拓扑排序
	 * @param graph
	 * @return
	 */
	public static List<Node> sortedTopology(Graph graph){

		List<Node> result = new ArrayList<>();
		// 记录节点入度的Map
		Map<Node, Integer> inMap = new HashMap();
		// 入度为0的节点
		Queue<Node> zeroQueue = new LinkedList<>();

		// 获取图中的所有节点
		Collection<Node> nodes = graph.nodes.values();
		for(Node node: nodes){
			inMap.put(node, node.in);
			// 有向无环图必存在入度为0的点
			if(node.in == 0){
				zeroQueue.add(node);
			}
		}

		while(!zeroQueue.isEmpty()){
			Node cur = zeroQueue.poll();
			result.add(cur);
			// cur 的nexts节点入度都减一
			for(Node next: cur.nexts){
				inMap.put(next, inMap.get(next) - 1);
				if(inMap.get(next) == 0){
					zeroQueue.add(next);
				}
			}
		}
		return result;
	}



	




}
