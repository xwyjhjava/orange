package com.dreams.algorithm.heap;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.heap
 * @date 2020/11/19 15:11
 * @description TODO
 */
public class HeapGreater<T> {

	private List<T> heap;
	private HashMap<T, Integer> indexMap;
	private int heapSize;
	private Comparator<? super T> comparator;

	// 初始化
	public HeapGreater(Comparator<T> comparator) {
		this.heap = new ArrayList<>();
		this.indexMap = new HashMap<>();
		this.heapSize = 0;
		this.comparator = comparator;
	}

	/**
	 * heap是否为空
	 * @return
	 */
	public boolean isEmpty(){
		return heapSize == 0;
	}

	/**
	 * heap 大小
	 * @return
	 */
	public int size(){
		return heapSize;
	}

	/**
	 * heap是否包含某一个key
	 * @param obj
	 * @return
	 */
	public boolean contains(T obj){
		return indexMap.containsKey(obj);
	}

	/**
	 * 获取堆顶元素
	 * @return
	 */
	public T peek(){
		return heap.get(0);
	}

	/**
	 * 向堆中添加元素
	 * @param obj
	 */
	public void push(T obj){
		heap.add(obj);
		// 更新indexMap
		indexMap.put(obj, heapSize);
		heapInsert(heapSize++);
	}


	/**
	 * 弹出堆中的元素
	 * @return
	 */
	public T pop() {
		T ans = heap.get(0);
		swap(0, heapSize - 1);
		indexMap.remove(ans);
		heap.remove(--heapSize);
		heapify(0);
		return ans;
	}

	/**
	 * 删除堆中的元素
	 * @param obj
	 */
	public void remove(T obj) {
		T replace = heap.get(heapSize - 1);
		int index = indexMap.get(obj);
		indexMap.remove(obj);
		heap.remove(--heapSize);
		if (obj != replace) {
			heap.set(index, replace);
			indexMap.put(replace, index);
			resign(replace);
		}
	}

	/**
	 *
	 * @param obj
	 */
	public void resign(T obj) {
		heapInsert(indexMap.get(obj));
		heapify(indexMap.get(obj));
	}

	/**
	 * 获取堆中的所有元素
	 * @return
	 */
	public List<T> getAllElements() {
		List<T> ans = new ArrayList<>();
		for (T c : heap) {
			ans.add(c);
		}
		return ans;
	}


	// ========================核心辅助方法=====================

	/**
	 *
	 * @param index
	 */
	public void heapInsert(int index){
		while(comparator.compare(heap.get(index), heap.get(index-1)) < 0){
			swap(index, index - 1);
			index = (index - 1 ) / 2;
		}
	}

	/**
	 *
	 * 调整堆结构
	 * @param index
	 */
	private void heapify(int index) {
		int left = index * 2 + 1;
		while (left < heapSize) {
			int best = left + 1 < heapSize && comparator.compare(heap.get(left + 1), heap.get(left)) < 0 ? (left + 1) : left;
			best = comparator.compare(heap.get(best), heap.get(index)) < 0 ? best : index;
			if (best == index) {
				break;
			}
			swap(best, index);
			index = best;
			left = index * 2 + 1;
		}
	}


	// =======================================================================

	// 交换堆中的两个值
	public void swap(int i, int j){
		T o1 = heap.get(i);
		T o2 = heap.get(j);

		heap.set(i, o2);
		heap.set(j, o1);

		indexMap.put(o1, j);
		indexMap.put(o2, i);
	}



	public static void main(String[] args) {
		Comparator<Integer> comp = (o1, o2) -> o1 - o2;
		int compare = comp.compare(1, 2);
		System.out.println("compare = " + compare);

	}






}
