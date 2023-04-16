package com.dreams.algorithm.graph;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.graph
 * @date 2020/10/20 11:13
 * @description TODO
 */

// 点的描述
public class Node {
	int value; // 编号
	int in; // 入度
	int out; // 出度
	List<Node> nexts;
	List<Edge> edges;

	public Node(int value){
		this.value = value;
		in = 0;
		out = 0;
		nexts = new ArrayList<>();
		edges = new ArrayList<>();
	}



}
