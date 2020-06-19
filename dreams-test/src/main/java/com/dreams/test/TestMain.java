package com.dreams.test;

import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.test
 * @date 2020/1/9 10:55
 * @description 单元测试
 */
public class TestMain {

    @Test
    public void uriTest(){

        String path = "/xiaoi/recommend";
        try {
            URI uri = new URI(path);
            String schema = uri.getScheme();
            System.out.println("schema = " + schema);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        List<String> list = new ArrayList();

    }


    @Test
    public void replaceTest(){
        String str = "font-family: 等线; baba:qqqq;";
        String replaceStr = str.replaceAll("^[font\\-family](.*?)[$;]", "-");
        System.out.println("replaceStr = " + replaceStr);


    }
}
