package com.dreams.thread.lock;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.thread.lock
 * @date 2020/9/24 17:22
 * @description CompletableFuture类的测试方法
 */
public class CompletableFutureExample {

	// TODO: 后续再改
	public static void main(String[] args) {

//		CompletableFuture.supplyAsync(() -> "hello")
//				.thenApply(s -> s + "world")
//				.thenAccept(System.out::println);

		String[] array1 = {"hello", "world", "world", "count", "hello"};

		CompletableFuture.runAsync(() -> {
			Arrays.stream(array1).map(ele -> {
				WordCount unit = new WordCount();
				unit.setKey(ele);
				unit.setNumber(1);
				return unit;
			});
		})
				.thenAccept(System.out::println);
	}


}


class WordCount{

	private String key;
	private Integer number;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Integer getNumber() {
		return number;
	}

	public void setNumber(Integer number) {
		this.number = number;
	}

	@Override
	public String toString() {
		return "WordCount{" +
				"key='" + key + '\'' +
				", number=" + number +
				'}';
	}
}
