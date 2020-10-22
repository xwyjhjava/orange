package com.dreams.algorithm.leetcode.simple;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.algorithm.leetcode.simple
 * @date 2020/10/21 11:26
 * @description TODO
 */
public class Problem_925_LongPressedName {

	public static void main(String[] args) {
		String name = "pyplrz";
		String typed = "ppyypllr";
		boolean flag = isLongPressedName(name, typed);
		System.out.println("result == " + flag);
	}


	/**
	 * Your friend is typing his name into a keyboard.  Sometimes, when typing a character c,
	 * the key might get long pressed, and the character will be typed 1 or more times.
	 *
	 * You examine the typed characters of the keyboard.  Return True if it is possible that it was your friends name,
	 * with some characters (possibly none) being long pressed.
	 *
	 * 基本实现思路： 双指针， 如果name和type当前值相等，则nameIndex和typeIndex同时+1
	 *               否则就不断增加typeIndex。
	 *               这种做法， 实践下来， 实现复杂， 且各种边界条件都需要考虑
	 *               没通过的case：  name=="pyplrz"  typed=="ppyypllr"
	 * @param name  1 <= name.length <= 1000
	 * @param typed 1 <= typed.length <= 1000
	 * @return
	 */
	public static boolean isLongPressedName_failed(String name, String typed) {

		if(name.length() > typed.length()){
			return false;
		}
		int nameIndex = 0;
		int namepreIndex = 0;
		int typedIndex = 0;
		while(nameIndex <= name.length() - 1 || typedIndex <= typed.length() - 1){
			if(nameIndex > name.length() - 1){
				nameIndex = name.length() - 1;
			}
			if(typedIndex > typed.length() - 1){
				typedIndex = typed.length() - 1;
			}
			char nameChar = name.charAt(nameIndex);
			char typedChar = typed.charAt(typedIndex);

			// 按位比较name和type
			// 如果对应位置的字符相同， 则nameIndex++, typeIndex++， 继续向后比较
			// 否则nameIndex停住， typedIndex向后， 比较typedIndex+1 和 typedIndex是否相同， 不同直接return false
			if(nameChar == typedChar){
				// nameIndex标记pre
				namepreIndex = nameIndex;
				nameIndex++;
				typedIndex++;
			}else{
				// 当前typedIndex是否和namepre相同
				while(typedIndex <= typed.length() - 1){
					// 当前typeIndex和pr相等， 说明重复按键了, 跳过
					// 不相等的时候, break出去
					if(typed.charAt(typedIndex) == name.charAt(namepreIndex)){
						typedIndex++;
					}else {
						// 更新后的值，做一次检查
						if(typed.charAt(typedIndex) == name.charAt(nameIndex)){
							break;
						}else{
							return false;
						}
					}
				}
			}
		}
		return true;
	}


	/**
	 * 在上面的版本基础上， 加以修改
	 * @param name
	 * @param typed
	 * @return
	 */
	public static boolean isLongPressedName(String name, String typed){

		if(name.length() > typed.length()){
			return false;
		}
		int nameIndex = 0;
		int namepreIndex = 0;
		int typedIndex = 0;

//		1. type字符至少和name一样长, 所以nameIndex的判断可以去掉
//		2. <= length - 1 等同于 < length
		while(typedIndex < typed.length()){

			// 3. name和type当前值相等，  nameIndex++的前提是nameIndex < name.length
			if(nameIndex < name.length() && name.charAt(nameIndex) == typed.charAt(typedIndex)){
				// 4. namepreIndex 可以用typeIndex - 1代替， 这样会节省一个变量
				namepreIndex = nameIndex;
				nameIndex++;
				typedIndex++;
			}else{
				// 5. typeIndex 和前一个字符相等， 则typeIndex++
				if(typed.charAt(typedIndex) == name.charAt(namepreIndex)){
					typedIndex++;
				}else{
					return false;
				}
			}
		}
		// 6. nameIndex是否走完
		return nameIndex == name.length();
	}



}
