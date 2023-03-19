package com.dreams.designpattern.chainOfResponsibilty;

/**
 * 场景： 加薪审批流程
 *
 * 直属领导 -> 部门领导 -> Boss
 *
 * 责任链模式：使多个对象都有机会处理请求，从而避免请求的发送者和接收者之间的耦合关系，
 *           将这个对象连成一条链，并沿着这条链传递该请求，直到有一个对象处理它为止
 *
 */
public class ChainMain {


    public static void main(String[] args) {

    }


}

class Client{

    public void wantedMoney(){
        System.out.println("申请加薪");
    }
}
