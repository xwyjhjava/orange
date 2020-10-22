package com.dreams.algorithm.graph;

import java.util.HashMap;
import java.util.HashSet;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.graph
 * @date 2020/10/20 11:39
 * @description TODO
 */
public class Graph {

	HashMap<Integer, Node> nodes;
	HashSet<Edge> edges;

	public Graph(){
		nodes = new HashMap<>();
		edges = new HashSet<>();
	}
}
