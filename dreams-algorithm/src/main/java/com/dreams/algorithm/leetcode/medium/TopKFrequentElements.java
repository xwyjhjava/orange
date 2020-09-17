package com.dreams.algorithm.leetcode.medium;

import java.util.*;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/9/7 16:15
 * @description 非空整数数组, 找出频率前K高的元素， 要求时间复杂度优于 O(NlogN)
 */
public class TopKFrequentElements {

	public static void main(String[] args) {
		int[] nums = new int[]{4,1,-1,2,-1,2,3};
//		topKFrequentByQueueSort(nums, 2);

//		Collections.swap(Arrays.asList(nums), 0, 1);

		List<Integer> numList = new ArrayList<>();
		numList.add(1);
		numList.add(2);
		numList.add(3);

		Collections.swap(numList, 0, 1);

		for(int num : numList){
			System.out.println("num = " + num);
		}

	}

	/**
	 * 本质上是一个排序问题，
	 * @param nums
	 * @param k
	 * @return
	 */
	public static int[] topKFrequent(int[] nums, int k){
		// 元素和频次Map
		Map<Integer, Integer> frequentMap = new HashMap<>();
		for (int i = 0; i < nums.length; i++) {
			// 频次
			Integer frequent = frequentMap.get(nums[i]);
			if(frequent == null){
				// map 中找不到， 则添加到Map中
				frequentMap.put(nums[i], 1);
			}else{
				//否则就+1
				frequentMap.put(nums[i], frequent+1);
			}
		}
		// 28-38 行写法优化
//		for (int num: nums){
//			frequentMap.put(num, frequentMap.getOrDefault(num, 0) + 1);
//		}

		// 对 Map 的value进行排序, 比较器实现
		Comparator<Map.Entry<Integer, Integer>> valueComparator = (o1, o2) -> o2.getValue() - o1.getValue();

		ArrayList<Map.Entry<Integer, Integer>> frequentList = new ArrayList<>(frequentMap.entrySet());
		// 排序， 双路排序
		Collections.sort(frequentList, valueComparator);

		int[] result = new int[k];

		Iterator<Map.Entry<Integer, Integer>> entryIterator = frequentMap.entrySet().iterator();

		// 循环list取前K个元素
		for (int i = 0; i < k; i++) {
			Map.Entry<Integer, Integer> entry = frequentList.get(i);
			// 取 频次top 的元素
			result[i] = entry.getKey();
		}

		for (int i = 0; i < result.length; i++) {
			System.out.println("i = " + result[i]);
		}

		return result;
	}



	// 小根堆
	public static int[] topKFrequentByQueueSort(int[] nums, int k){
		// 初始化HashMap
		Map<Integer, Integer> occurrences = new HashMap<Integer, Integer>();
		for(int num : nums){
			occurrences.put(num, occurrences.getOrDefault(num,0) + 1);
		}

//		for(Map.Entry<Integer, Integer> entrySet :occurrences.entrySet()){
//			Integer key = entrySet.getKey();
//			Integer value = entrySet.getValue();
//			System.out.println("key = " + key + " || " + " value = " + value);
//		}
//		System.out.println("===========================");

		// 队列元素不允许是null， 且元素需要可比较
		// 实现数组的比较器， count升序
//		todo 理解 lambada的表达方式(如何控制升序和降序)
		PriorityQueue<int[]> queue = new PriorityQueue<>(Comparator.comparingInt(o -> o[1]));

		// 遍历Map
		for(Map.Entry<Integer, Integer> entry: occurrences.entrySet()){
			Integer ele = entry.getKey();
			Integer count = entry.getValue();
			if(queue.size() == k){ // == 在算法上符合要求
				// queue 队顶元素和新来的元素比较
				if(queue.peek()[1] < count){
					queue.poll();  // 新元素count值大于队顶元素count值时， 弹出队顶元素
					queue.offer(new int[]{ele, count}); // 新元素count加入队列
				}
			}else{ // queue.size <  k
				// queue中依次添加元素
				queue.offer(new int[]{ele, count});
			}
		}

		int[] result = new int[k];
		for (int i = 0; i < k; i++) {
			result[i] = queue.poll()[0];
		}

		for(int res: result){
			System.out.println("res = " + res);
		}

		return result;
	}

	// 快排变种实现
	public int[] topKFrequentByQuickSort(int[] nums, int k){
		Map<Integer, Integer> occurrences =  new HashMap<>();
		for(int num : nums){
			occurrences.put(num, occurrences.getOrDefault(num, 0) + 1);
		}

		return null;

	}

	/**
	 *
	 * @param values
	 * @param start
	 * @param end
	 * @param ret
	 * @param retIndex
	 * @param k
	 */
	public void qsort(List<int[]> values, int start, int end, int[] ret, int retIndex, int k){
		//随机计算出一个划分左右的下标
		int picked = (int) (Math.random() * (end - start + 1)) + start;
		System.out.println("picked = " + picked);

		Collections.swap(values, picked, start);

	}





}
