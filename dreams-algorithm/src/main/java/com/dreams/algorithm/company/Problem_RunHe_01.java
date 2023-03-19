package com.dreams.algorithm.company;

import java.math.BigDecimal;

public class Problem_RunHe_01 {


    public static void main(String[] args) {

//        BigDecimal decimal = new BigDecimal(10000000);
//        BigDecimal pow = decimal.pow(2);
//        long result = pow.longValue();
//        System.out.println(result);

        calculate();
    }


    public static void calculate(){
        long value = 1000000000;
        long result = value * value;
        System.out.println("result = " + result);
    }
}
