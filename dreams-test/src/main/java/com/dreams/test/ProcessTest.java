package com.dreams.test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.test
 * @date 2020/6/28 16:58
 * @description 处理测试
 */
public class ProcessTest {

    private static String line = "788d9566ba72ebb2|1589285988490732|4af578c3bde7cf64|59fd0d92afefca31|572|OrderCenter|/apache/|192.168.18.15|db.instance=db&component=java-jdbc&db.type=h2&span.kind=client&__sql_id=1x7lx2l&peer.address=localhost:8082";

    private static String line2 = "788d9566ba72ebb2|1589285988490754|6dbf6d4fa2904fb3|ebc9f6feea3645f|497|OrderCenter|db.AlertTemplateDao.searchByComplexByPage(..)|192.168.18.27|http.status_code=200&component=java-spring-rest-template&span.kind=client&http.url=http://localhost:9001/getItem?id=5&peer.port=9001&http.method=GET";






    private static List<Map<String, List<String>>> BATCH_TRACE_LIST = new ArrayList<>();

    private static int BATCH_COUNT = 15;
    public ProcessTest() {

    }

    public static void main(String[] args) {

        ArrayList<String> dataList = new ArrayList<>();
        dataList.add(line);
        dataList.add(line2);


        for (int i = 0; i < BATCH_COUNT; i++) {
            BATCH_TRACE_LIST.add(new ConcurrentHashMap<>(20000));
        }

        Map<String, List<String>> traceMap = BATCH_TRACE_LIST.get(0);

        int count = 0;

        for (int a = 0; a < dataList.size(); a++) {

            count ++;

            Set<String> badTraceIdList = new HashSet<>(5000);

            String[] cols = dataList.get(a).split("\\|");
            if (cols != null && cols.length > 1) {
                String traceId = cols[0];
                List<String> spanList = traceMap.get(traceId);
                if (spanList == null) {
                    spanList = new ArrayList<>();
                    traceMap.put(traceId, spanList);
                }
                spanList.add(line);
                if (cols.length > 8) {
                    // tag不在尾部， 通过下标的方式不能确定
                    // 穷举的方式找到符合条件的tags
                    for (int i = 0; i < cols.length; i++) {
                        String tags = cols[i];
                        // 从tags中找到符合条件的traceId
                        if (tags != null) {
                            if (tags.contains("error=1")) {
                                badTraceIdList.add(traceId);
                            } else if (tags.contains("http.status_code=") && tags.indexOf("http.status_code=200") < 0) {
                                badTraceIdList.add(traceId);
                            }
                        }
                    }
                }



            }
        }


        System.out.println("traceMap = " + traceMap.size());

    }

}
