package com.dreams.algorithm.leetcode.simple;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/12/6 22:22
 * @description TODO
 */
public class Problem_118_PascalsTriangle {

	public static void main(String[] args) {
		int numRows = 5;
		List<List<Integer>> generate = generate(numRows);
		generate.size();
	}

	/**
	 *
	 *
	 * Given a non-negative integer numRows, generate the first numRows of Pascal's triangle.
	 *
	 * In Pascal's triangle, each number is the sum of the two numbers directly above it.(杨辉三角)
	 * 来源：力扣（LeetCode）
	 * 链接：https://leetcode-cn.com/problems/pascals-triangle
	 * 著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
	 * @param numRows
	 * @return
	 */
	public static List<List<Integer>> generate(int numRows){

		List<List<Integer>> ans = new ArrayList<>();
		for(int i = 0; i < numRows; i++){
			List<Integer> cur = new ArrayList<>();
			for(int j = 0; j <= i; j++){
				// j下标的最大值是i
				// 0位置和i位置都填充1
				if(j == 0 || j == i){
					cur.add(1);
				}else{
					cur.add(ans.get(i-1).get(j-1) + ans.get(i-1).get(j));
				}
			}
			ans.add(cur);
		}
		return ans;
	}


}
