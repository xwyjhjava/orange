package com.dreams.algorithm.brushquestion;

import java.io.File;
import java.util.Stack;

/**
 *  给定一个文件目录地址
 *  写一个函数统计这个目录下所有的文件数量并返回
 *  隐藏文件也算，但是文件夹不算
 */
public class Problem_02 {

    // 宽度优先遍历 bfs  非递归实现
    public static int countFile(String path){
        File root = new File(path);
        if(!root.exists()){
            System.out.println("文件夹不存在");
            return 0;
        }
        if(!root.isDirectory() && !root.isFile()){
            System.out.println("输入不是一个文件");
            return 0;
        }
        int total = 0;
        Stack<File> stack = new Stack<>();
        stack.add(root);

        while(!stack.isEmpty()){
            File curFile = stack.pop();
            // 遍历
            for(File f: curFile.listFiles()){
                if(f.isFile() && f.getName().endsWith(".java")){
                    total++;
                }
                if(f.isDirectory()){
                    stack.push(f);
                }
            }
        }
        return total;
    }


    // bfs 递归实现
    public static int bfs(String path){
        File root = new File(path);
        if(!root.exists()){
            return 0;
        }
        int total = 0;

        if(root.isDirectory()){
            File[] files = root.listFiles();
            for (File f: files){
                total += bfs(f.getPath());
            }
        }
        if(root.isFile() && root.getName().endsWith(".java")){
            total++;
        }
        return total;
    }


    public static void main(String[] args) {
        String path = "/Users/mingzhao/IdeaProjects/dreams";
        int ans = bfs(path);
        System.out.println("ans == " + ans);
        System.out.println("ans ==" + countFile(path));
    }
}
