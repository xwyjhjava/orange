package com.dreams.algorithm.dynamic;

public class RobotWay {

    //机器人走路问题
    public static int robot_process(int N, int start, int aim, int K){
        return process_try(start, K, aim, N);
    }
    /**
     *
     * @param cur 机器人当前来到的位置是cur
     * @param rest 机器人还有rest步需要去走
     * @param aim 最终的目标aim
     * @param N 有哪些位置
     * @return 机器人从cur出发，走过rest步之后，最终在aim的方法数，是多少 ？
     */
    public static int process_try(int cur, int rest, int aim, int N){
        if(rest == 0){ //如果已经不需要走了，走完了
            return cur == aim ? 1 : 0;
        }
        // rest > 0
        if(cur == 1){
            return process_try(2, rest - 1, aim, N);
        }
        if(cur == N){
            return process_try(N - 1, rest - 1, aim, N);
        }
        return process_try(cur - 1, rest - 1, aim, N) +
                process_try(cur + 1, rest - 1, aim, N);

    }

    // 缓存法优化
    public static int robot_process2(int N, int start, int aim, int K){
        int[][] dp = new int[N + 1][K + 1];
        for (int i = 0; i <= N; i++) {
            for(int j = 0; j <= K; j++){
                dp[i][j] = -1;
            }
        }
        //dp就是缓存表
        //dp[cur][rest] == -1 表示process之前没算过
        //dp[cur][rest] !== -1 表示process之前算过，返回值dp[cur][rest]
        return process_try2(start, K, aim, N, dp);

    }


    /**
     * cur : [1, N]
     * rest : [0, K]
     * @return
     */
    public static int process_try2(int cur, int rest, int aim, int N, int[][]dp){
        if(dp[cur][rest] != -1){
            return dp[cur][rest];
        }
        //之前没算过
        if(rest == 0){

        }

        return 0;
    }



    public static int process_try3(int N, int start, int aim, int K){
        // 参数检查
        if(N < 1 || start < 1 || start > N || aim < 1 || aim > N || K < 1){
            return -1;
        }

        // 初始化dp
        int[][] dp = new int[N + 1][K + 1];
        // 填充第0列
        dp[aim][0] = 1;

        // 填充其他格
        for(int rest = 1; rest <= K; rest++){ //列

            // 第一行的依赖
            dp[1][rest] = dp[2][rest - 1];
            // 第一行和最后一行都单独计算了，所以边界条件是 [2,N)
            for(int cur = 2; cur < N; cur++){ //行
                dp[cur][rest] = dp[cur - 1][rest - 1] +
                        dp[cur + 1][rest - 1];
            }
            // 最后一行的依赖
            dp[N][rest] = dp[N - 1][rest - 1];

        }
        return dp[start][K];
    }




    public static void main(String[] args) {
        int ans = process_try3(5, 2, 4, 6);
        System.out.println("ans = " + ans);
    }
}
