package com.dreams.algorithm.tree;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.tree
 * @date 2020/9/27 17:41
 * @description TODO
 */
public class TestMain {

	public static void main(String[] args) {

		int a = 0;
		a++;
		System.out.println("a = " + a);


		Map<String, Integer> map = new HashMap<>();
		map.put("a", a + 1);
		map.put("b", a++);
		System.out.println("map a = " + map.get("a"));
		System.out.println("map b = " + map.get("b"));


	}

}
