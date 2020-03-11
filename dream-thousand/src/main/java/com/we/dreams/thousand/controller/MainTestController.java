package com.we.dreams.thousand.controller;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;

/**
 * @author ming
 * @version V1.0
 * @Package com.we.dreams.thousand.controller
 * @date 2020/3/11 9:40
 * @description controller 测试
 */
@Controller
public class MainTestController {

    public static int memory_threadhold = 1024 * 1024 * 3;

    @GetMapping("test")
    public void getRequest(HttpServletRequest request, HttpServletResponse response){

        DiskFileItemFactory factory = new DiskFileItemFactory();
        factory.setSizeThreshold(memory_threadhold);

        ServletFileUpload upload = new ServletFileUpload(factory);

        try {
            List<FileItem> items = upload.parseRequest(request);




        } catch (FileUploadException e) {
            e.printStackTrace();
        }

    }


    @GetMapping("table")
    public String getTableTest(){
        return "table_test";
    }
}
