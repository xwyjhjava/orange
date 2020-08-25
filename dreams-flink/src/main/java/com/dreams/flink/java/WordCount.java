package com.dreams.flink.java;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @version V1.0
 */
public class WordCount {

	public static void main(String[] args) throws Exception {

		/** get default environment.  */
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Person> flintstones = env.fromElements(
				new Person("Fred", 35),
				new Person("Wilma", 35),
				new Person("Pebbles", 2)
		);

		/** socket流. */
		DataStream<String> lines = env.socketTextStream("localhost", 9999);

		/** 文件流. */
		DataStream<String> lineFile = env.readTextFile("file:///path");

		/** 过滤条件. */
		DataStream<Person> adults = flintstones.filter(new FilterFunction<Person>() {
			@Override
			public boolean filter(Person person) throws Exception {
				return person.age >= 18;
			}
		});

		adults.print();

		env.execute();

	}

	/**
	 * Person.
	 */
	public static class Person {
		public String name;
		public Integer age;

		public Person(String name, Integer age) {
			this.name = name;
			this.age = age;
		}

		@Override
		public String toString() {
			return "Person{" +
					"name='" + name + '\'' +
					", age=" + age +
					'}';
		}
	}
}
