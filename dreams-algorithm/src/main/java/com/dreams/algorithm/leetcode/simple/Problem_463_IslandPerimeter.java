package com.dreams.algorithm.leetcode.simple;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/10/30 11:35
 * @description TODO
 */
public class Problem_463_IslandPerimeter {

	public static void main(String[] args) {

	}


	/**
	 * You are given row x col grid representing a map where grid[i][j] = 1
	 * represents land and grid[i][j] = 0 represents water.
	 *
	 * Grid cells are connected horizontally/vertically (not diagonally).
	 * The grid is completely surrounded by water, and there is exactly
	 * one island (i.e., one or more connected land cells).
	 *
	 * The island doesn't have "lakes", meaning the water inside isn't
	 * connected to the water around the island. One cell is a square
	 * with side length 1. The grid is rectangular, width and height
	 * don't exceed 100. Determine the perimeter of the island.
	 *
	 * 解法： 迭代法， 依次遍历每个水域, 观察上下左右四个方向上是否有水域， 没有则记为边长
	 * @param grid   row == grid.length
	 *               col == grid[i].length
	 *               1 <= row,col <= 100
	 *               grid[i][j] is 0 or 1
	 * @return
	 *
	 */
	public int islandPerimeter(int[][] grid){
		int height = grid.length;
		int width = grid[0].length;

		int ans = 0;
		//
		for(int i = 0; i < height; i++){
			for(int j = 0; j < width; j++){
				int cur = grid[i][j];
				// 当前格子是岛屿
				if(cur == 1){
					//上
					if(j - 1 < 0 || grid[i][j - 1] == 0){
						ans++;
					}
					// 下
					if(j + 1 >= width || grid[i][j + 1] == 0){
						ans++;
					}
					// 左
					if(i - 1 < 0 || grid[i - 1][j] == 0){
						ans++;
					}
					// 右
					if(i + 1 >= height || grid[i + 1][j] == 0){
						ans++;
					}
				}else{
					continue;
				}
			}
		}

		return ans;
	}


	public static int search(int i, int j, int height, int width){
		int ans = 0;

		int left = i - 1;
		int right = i + 1;
		int top = j - 1;
		int bottom = j + 1;

		return ans;
	}

}
