package com.dreams.algorithm.leetcode.medium;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.medium
 * @date 2020/11/5 13:42
 * @description TODO
 */
public class Problem_127_WordLadder {

	public static void main(String[] args) {
		String beginWord = "hit";
		String endWord = "cog";
//		["hot","dot","dog","lot","log","cog"]
		List<String> wordList = new ArrayList<String>(){{
			add("hot");
			add("dot");
			add("tog");
			add("cog");
//			add("log");
//			add("cog");
		}};

		int len = ladderLength(beginWord, endWord, wordList);
		System.out.println("len = " + len);

	}

	/**
	 *
	 * Given two words (beginWord and endWord), and a dictionary's word list, find the length of
	 * shortest transformation sequence from beginWord to endWord, such that:
	 *
	 * Only one letter can be changed at a time.
	 * Each transformed word must exist in the word list.
	 * Note:
	 *      Return 0 if there is no such transformation sequence.
	 *      All words have the same length.
	 *      All words contain only lowercase alphabetic characters.
	 *      You may assume no duplicates in the word list.
	 *      You may assume beginWord and endWord are non-empty and are not the same.
	 *
	 * @param beginWord
	 * @param endWord
	 * @param wordList
	 * @return
	 */
	public static int ladderLength(String beginWord, String endWord, List<String> wordList) {
		// 字典set
		Set<String> dict = new HashSet<>(wordList);

		Set<String> startSet = new HashSet<>();
		Set<String> endSet = new HashSet<>();
		Set<String> visitSet = new HashSet<>();
		// beginWord 进入 startSet
		startSet.add(beginWord);
		// endWord 进入 endSet
		if(dict.contains(endWord)){
			endSet.add(endWord);
		}else{
			return 0;
		}

		for(int len = 2; !startSet.isEmpty(); len++){
			// 下一层
			Set<String> nextSet = new HashSet<>();
			// 遍历startSet
			for(String word: startSet){
				// 当前单词变换每一个位, 从'a' -> 'z'
				for(int j = 0; j < word.length(); j++){
					char[] cur = word.toCharArray();
					for(char c = 'a'; c <= 'z'; c++){
						// 去掉当前自身的一种, 26 -> 25
						if(c != word.charAt(j)){
							// 组装word
							cur[j] = c;
							String next = String.valueOf(cur);
							// 如果next出现在了endSet中, 说明此时相遇了, 返回len
							if(endSet.contains(next)){
								return len;
							}

							//next 在dict里
							if(dict.contains(next) && !visitSet.contains(next)){
								nextSet.add(next);
								visitSet.add(next);
							}

						}
					}
				}
			}
			// 更新startSet
			// 如果nextSet更小, 则取nextSet作为startSet
			// 如果startSet更小,则取endSet作为startSet
			startSet = nextSet.size() > endSet.size() ? endSet : nextSet;

			// 如果 startSet == nextSet, 则 endSet = endSet
			// 否则 endSet = nextSet
			endSet = startSet == nextSet ? endSet : nextSet;
		}
		return 0;
	}
}
