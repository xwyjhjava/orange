package com.dreams.algorithm.brushquestion;

/**
 * 给定一个有序数组arr， 代表坐落在X轴上的点
 * 给定一个正数K，代表绳子长度
 * 返回绳子最多压中几个点？
 * 即使绳子边缘出盖住点也算盖住
 *
 */
public class Problem_01 {


    public static int answer(int[] arr, int K){
        if(arr == null || arr.length < 1){
            return 0;
        }

        return process2(arr, K);
    }
    // 绳子末尾不必压在不存在的点上
    // 二分法
    public static int process1(int[] arr, int K){
        int res = 1;
        for(int i = 0; i < arr.length; i++){
            int nearest = nearestIndex(arr, i, arr[i] - K);
            res = Math.max(res, i - nearest + 1);
        }

        return res;
    }

    public static int nearestIndex(int[] arr, int R, int value){
        int L = 0;
        int index = R;
        while(L <= R){
            int mid = L + ((R - L) >> 1);
            if(arr[mid] >= value){
                index = mid;
                R = mid - 1;
            }else{
                L = mid + 1;
            }
        }
        return index;
    }


    // 滑动窗口
    public static int process2(int[] arr, int K){
        int left = 0;
        int right = 0;
        int max = 0;
        int N = arr.length;

        while(left < N){
            while(right < N && arr[right] - arr[left] <= K){
                right++;
            }
            max = Math.max(max, (right -left + 1));
            left++;
        }

        return max;

    }




    public static void main(String[] args) {

    }

}
