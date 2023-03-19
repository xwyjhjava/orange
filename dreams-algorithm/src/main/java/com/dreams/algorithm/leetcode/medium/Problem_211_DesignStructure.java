package com.dreams.algorithm.leetcode.medium;

import java.util.ArrayList;
import java.util.List;

/**
 *
 请你设计一个数据结构，支持 添加新单词 和 查找字符串是否与任何先前添加的字符串匹配 。

 实现词典类 WordDictionary ：

 WordDictionary() 初始化词典对象
 void addWord(word) 将 word 添加到数据结构中，之后可以对它进行匹配
 bool search(word) 如果数据结构中存在字符串与word 匹配，则返回 true ；否则，返回 false 。word 中可能包含一些 '.' ，每个.都可以表示任何一个字母。

 来源：力扣（LeetCode）
 链接：https://leetcode-cn.com/problems/design-add-and-search-words-data-structure
 著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
 */

public class Problem_211_DesignStructure {


    //解法：字典树(前缀树)

    public static void main(String[] args) {

    }
}

class WordDictionary {

    private List dictionary;

    public WordDictionary() {
        dictionary = new ArrayList<String>();

    }

    public void addWord(String word) {
        dictionary.add(word);
    }

    public boolean search(String word) {

        dictionary.indexOf(word);

        if(dictionary.contains(word)){
            return true;
        }
        return false;
    }
}
