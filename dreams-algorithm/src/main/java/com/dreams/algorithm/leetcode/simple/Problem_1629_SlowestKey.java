package com.dreams.algorithm.leetcode.simple;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/10/27 15:53
 * @description TODO
 */
public class Problem_1629_SlowestKey {

	public static void main(String[] args) {
		int[] releaseTimes = {12,23,36,46,62};
		String keysPressed = "spuda";
		char ans = slowestKey(releaseTimes, keysPressed);
		System.out.println("ans = " + ans);
	}


	static int[] ans = new int[2];
	public static char slowestKey(int[] releaseTimes, String keysPressed) {
		char[] str = keysPressed.toCharArray();
		// 第一位存放字符， 第二位存放持续时间

		ans[0] = str[0];
		ans[1] = releaseTimes[0];

		for(int i = 1; i < releaseTimes.length; i++){
			//当前字符的持续时间
			int curTime = releaseTimes[i] - releaseTimes[i - 1];
			int oldTime = ans[1];

			int curChar = str[i];
			int oldChar = ans[0];
			// 比较持续时间
			if(curTime > oldTime){
				ans[0] = curChar;
				ans[1] = curTime;
			}else if(curTime == oldTime){
				ans[0] = curChar - oldChar >= 0 ? curChar : oldChar;
				ans[1] = curTime;
			}

		}
		return (char)ans[0];
	}
}
