package com.dreams.algorithm.dynamic;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.dynamic
 * @date 2020/10/26 16:35
 * @description TODO
 */
public class DynamicCore {

	/**
	 * 打印一个字符串的全部子序列
	 * @param arr
	 * @param index
	 * @param ans
	 * @param path
	 */
	public static void process1(char[] arr, int index, List<String> ans, String path){
		// base
		if(index == arr.length){
			ans.add(path);
			return;
		}
		// 不选择当前字符
		String no = path;
		process1(arr, index + 1, ans, no);
		// 选择当前字符
		String yes = path + String.valueOf(arr[index]);
		process1(arr, index + 1, ans, yes);


	}

	/**
	 * 打印一个字符串的全部排序，要求不要出现重复的子排列
	 * @param arr
	 * @param i
	 * @param ans
	 */
	public static void process2(char[] arr, int i, List<String> ans){
		if(i == arr.length){
			ans.add(String.valueOf(arr));
			return;
		}

		boolean[] visited = new boolean[26];
		for (int j = i; j < arr.length; j++) {
			if(!visited[arr[j] - 'a']){
				visited[arr[j] - 'a'] = true;
				swap(arr, i, j);
				process2(arr, i + 1, ans);
				swap(arr, i, j);
			}
		}
	}

	public static void swap(char[] arr, int i, int j){
		char tmp = arr[i];
		arr[i] = arr[j];
		arr[j] = tmp;
	}


	public static void main(String[] args) {

	}





}
