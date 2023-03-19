package com.dreams.algorithm.company;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Problem_countNumberOfCharacter {



    public int process(String str){
        if(str == null || str.length() < 1){
            return -1;
        }

        // 大小写字母都有
        char[] chars = str.toCharArray();
        int[] count = new int[52];

        for(int i = 0; i < chars.length; i++){
            //  大小写判断
            char cur = chars[i];
            if(cur >= 'a' && cur <= 'z'){
                count[cur - 'a']++;
            }else{
                count[chars[i] - 'A' + 26]++;
            }
        }

        return 0;
    }


    public static void process2(String str){
        if(str == null || str.length() < 1){
            return;
        }

        char[] chars = str.toCharArray();
        Map<Character, Integer> map = new HashMap<>();

        for(char c : chars){
            map.put(c, map.getOrDefault(c, 0) + 1);
        }

        List<Map.Entry<Character, Integer>> list = map.entrySet().stream().filter(e -> e.getKey() >= 'a' || e.getKey() >= 'A')
                .sorted((o1, o2) -> {
                    if (o1.getValue() != o2.getValue()) {
                        return o2.getValue() - o1.getValue();
                    } else {
                        return o1.getKey() - o2.getKey();
                    }
                })
                .collect(Collectors.toList());


        StringBuilder builder = new StringBuilder();
        for(Map.Entry<Character, Integer> entry: list){
            builder.append(entry.getKey() + ":" + entry.getValue());
        }

        System.out.println(builder.toString());


    }


    public static void main(String[] args) {
        String str = "xxxxYY";

        process2(str);

    }

}
