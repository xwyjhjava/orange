package com.dreams.algorithm.graph;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.graph
 * @date 2020/10/20 11:38
 * @description TODO
 */
public class Edge {

	int weight;
	Node from;
	Node to;

	public Edge(int weight, Node from, Node to) {
		this.weight = weight;
		this.from = from;
		this.to = to;
	}
}
