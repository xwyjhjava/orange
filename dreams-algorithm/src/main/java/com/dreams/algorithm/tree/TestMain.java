package com.dreams.algorithm.tree;

import java.util.Arrays;
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

		int result = (0 - 1) / 2;
		System.out.println("result = " + result);

		String word = "abc";

	}

	public static int getDigit(int x, int d){
		return ((x / ((int)Math.pow(10, d - 1))) % 10);
	}

}
