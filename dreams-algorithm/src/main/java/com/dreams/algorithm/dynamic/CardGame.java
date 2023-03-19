package com.dreams.algorithm.dynamic;

/**
 *
 * (范围尝试模型)
 *
 * 给定一个数型数组arr，代表数值不同的纸牌排成一条线
 * 玩家A和玩家B依次拿走每张纸牌
 * 规定玩家A先拿，玩家B后拿
 * 但是每个玩家每次只能拿走最左或最后的纸牌
 * 玩家A和B都绝顶聪明
 * 请返回最后获胜者的分数
 *
 */
public class CardGame {


    public static int win1(int[] arr){
        if(arr == null || arr.length < 1){
            return 0;
        }
        int first = f1(arr, 0, arr.length - 1);
        int second = g1(arr, 0, arr.length - 1);

        return Math.max(first, second);
    }

    // arr[L..R]，先手获得的最好分数返回
    public static int f1(int[] arr, int L, int R){
        if(L == R){
            return arr[L];
        }
        int p1 = arr[L] + g1(arr, L + 1, R);
        int p2 = arr[R] + g1(arr, L, R - 1);
        return Math.max(p1, p2);
    }
    // arr[L..R]，后手获得的最好分数返回
    public static int g1(int[] arr, int L, int R){
        if(L == R){
            return 0;
        }
        // 对手会将最小值的最优给到后手
        int p1 = f1(arr, L + 1, R);
        int p2 = f1(arr, L, R - 1);
        return Math.min(p1, p2);
    }



    // ===============================================
    // 缓存优化


    public static int win2(int[] arr){
        int N = arr.length;
        int[][] fmap = new int[N][N];
        int[][] gmap = new int[N][N];

        // 初始化，标记是否计算过
        for(int i = 0; i <= N - 1; i++){
            for(int j = 0; j <= N - 1; j++){
                fmap[i][j] = -1;
                gmap[i][j] = -1;
            }
        }
        int first = f2(arr, 0, N - 1, fmap, gmap);
        int second = g2(arr, 0, N - 1, fmap, gmap);
        return Math.max(first, second);

    }


    // arr[L..R]，先手获得的最好分数返回
    public static int f2(int[] arr, int L, int R, int[][] fmap, int[][] gmap){

        if(fmap[L][R] != -1){
            return fmap[L][R];
        }
        int ans = 0;
        if(L == R){
            ans = arr[L];
        }else {
            int p1 = arr[L] + g2(arr, L + 1, R, fmap, gmap);
            int p2 = arr[R] + g2(arr, L, R - 1, fmap, gmap);
            ans = Math.max(p1, p2);
        }
        fmap[L][R] = ans;

        return ans;
    }
    // arr[L..R]，后手获得的最好分数返回
    public static int g2(int[] arr, int L, int R, int[][] fmap, int[][] gmap){

        if(gmap[L][R] != -1){
            return gmap[L][R];
        }
        int ans = 0;

        if(L != R) {
            // 对手会将最小值的最优给到后手
            int p1 = f2(arr, L + 1, R, fmap, gmap);
            int p2 = f2(arr, L, R - 1, fmap, gmap);
            ans = Math.min(p1, p2);
        }
        gmap[L][R] = ans;
        return ans;
    }



    // =========================================================
    // 动态规划
    public static int win3(int[] arr){

        int N = arr.length;
        int[][] fmap = new int[N][N];
        int[][] gmap = new int[N][N];

        // 填充fmap的对角线值
        for (int i = 0; i < N; i++) {
            fmap[i][i] = arr[i];
        }
        // gmap对角线值已默认填充为0

        for(int col = 1; col < N; col++){
            int L = 0;
            int R = col;

            while(R < N) {
                fmap[L][R] = Math.max(arr[L] + gmap[L + 1][R], arr[R] + gmap[L][R - 1]);
                gmap[L][R] = Math.min(fmap[L + 1][R], fmap[L][R - 1]);
                L++;
                R++;
            }
        }
        return Math.max(fmap[0][N - 1], gmap[0][N - 1]);
    }






    public static void main(String[] args) {

        int[] arr = {5, 7, 4, 5, 8, 1, 6, 0, 3, 4, 6, 1, 7};
        System.out.println("ans = " + win1(arr));
        System.out.println("ans = " + win2(arr));
        System.out.println("ans = " + win3(arr));

    }




}
