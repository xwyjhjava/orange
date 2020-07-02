package com.dreams.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.test
 * @date 2020/7/1 13:35
 * @description currentHashMap多线程测试
 */
public class CurrentThreadTest {





    public CurrentThreadTest() {
    }

    public static void main(String[] args) throws InterruptedException {

        ClientProcess.init();
        Thread clientThread = new Thread(new ClientProcess(), "client process thread");
        clientThread.start();

        Thread.sleep(100);

        Thread backendThread = new Thread(new BackendProcess(), "backend process");
        backendThread.start();

    }






}


class ClientProcess implements Runnable{

    private static List<Map<String , List<String>>> BATCH_TRACE_LIST = new ArrayList<>();

    private static int BATCH_COUNT = 15;

    public static void init(){
        for (int i = 0; i < BATCH_COUNT; i++) {
            BATCH_TRACE_LIST.add(new ConcurrentHashMap<>(20000));
        }
    }

    @Override
    public void run() {
        System.out.println("ClientProcess.run");
        Map<String, List<String>> traceMap = BATCH_TRACE_LIST.get(0);

        int count = 0;
        String traceId = "1";
        while(true) {
            count ++;
            try {
//                Thread.sleep(10);

                String line = "abc";

                List<String> spanList = traceMap.get(traceId);
                if(spanList == null){
                    spanList = new ArrayList<>();
                    traceMap.put(traceId, spanList);
                }
                spanList.add(line);


                if(count % 20000 == 0){
                    traceMap = BATCH_TRACE_LIST.get(0);

                    System.out.println("client traceMap size = " + traceMap.size());
                    if(traceMap.size() > 0){
                        while (true){
                            Thread.sleep(10);
                            if(traceMap.size() == 0){
                                System.out.println("client traceMap.size() == 0");
                                break;
                            }
                        }
                    }
                }



            } catch (InterruptedException e) {
                e.printStackTrace();
            }



        }

    }

    public static void getWrongTrace(){
        BATCH_TRACE_LIST.get(0).clear();

    }
}


// 创建
class BackendProcess implements Runnable{

    @Override
    public void run() {

        System.out.println("BackendProcess.run");
        try {
            while (true) {
                Thread.sleep(2000);
                System.out.println("clean traceMap");
                ClientProcess.getWrongTrace();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}

