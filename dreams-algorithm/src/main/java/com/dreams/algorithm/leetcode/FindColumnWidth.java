package com.dreams.algorithm.leetcode;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class FindColumnWidth {

    public static void main(String[] args) {
        test();
    }


    public static int[] findColumnWidth(int[][] grid) {
        int[] ans = new int[grid[0].length];
        for (int i = 0 ; i < grid.length; i++) {
            for (int j = 0; j < grid[i].length; j++) {
                int val = grid[i][j];
                ans[j] = Math.max(Integer.toString(val).toCharArray().length, ans[j]);
            }
        }
        return ans;
    }

    public static long[] findPrefixScore(int[] nums) {
        long ans[] = new long[nums.length];
        int max = nums[0];
        ans[0] = max << 1;
        for (int i = 1; i < nums.length; i++) {
            max = Math.max(max, nums[i]);
            ans[i] = nums[i] + max + ans[i - 1];
        }

        return ans;
    }


    public static TreeNode replaceValueInTree(TreeNode root) {
        TreeNode ans = new TreeNode();
        ans.val = 0;
        ans.left.val = 0;
        ans.right.val = 0;

        root = root.left;
        TreeNode bro = root.right;

        TreeNode head1 = root.left;
        TreeNode head2 = ans.left;

        while (root != null) {
            ans.val = (bro.left != null ? bro.left.val : 0 ) + (bro.right != null ? bro.right.val : 0 );

        }


        return ans;
    }

    public static void bfs() {

    }

    public static void dfs() {
        Map<Integer, Integer> map = new HashMap<>();
        int threshold = 0;
        AtomicInteger ans = new AtomicInteger(-1);

        map.forEach((key, value) -> {
            if (value >= threshold) {
                ans.set(key);
            }
        });
        ans.get();


        int[] nums = new int[]{};
        int[] newnums  = Arrays.copyOfRange(nums, 0, 7);
        Arrays.sort(newnums);
    }

    public static void test() {
        int[] arr = {1,1,2,800,1,1};
        int[] sorted = Arrays.copyOf(arr, arr.length);
        Arrays.sort(sorted);
        int max = sorted[sorted.length - 1];
        System.out.println(max);
        int query = query(arr, 0, 3, 3, max);
        System.out.println("query = " + query);


    }

    public static int query(int[] arr, int left, int right, int threshold, int max) {
        int index = Math.max(max, right);
        System.out.println("index = " + index);
        int[] newnums  = Arrays.copyOfRange(arr, left, right + 1);
        int[] count = new int[index + 1];
        for (int i = 0; i < newnums.length; i++) {
            count[newnums[i]]++;
        }

        int ans = -1;
        for (int k = 0; k < count.length; k++) {
            if (count[k] >= threshold) {
                ans = k;
            }
        }
        return ans;
    }


}


class TreeNode {
    int val;
    TreeNode left;
    TreeNode right;
    TreeNode() {}
    TreeNode(int val) { this.val = val; }
    TreeNode(int val, TreeNode left, TreeNode right) {
          this.val = val;
          this.left = left;
          this.right = right;
    }
}
