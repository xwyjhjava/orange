package com.we.dreams.thousand.controller;

import com.we.dreams.thousand.mapper.TbTaskMapper;
import com.we.dreams.thousand.model.TbTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author ming
 * @version V1.0
 * @Package com.we.dreams.thousand.controller
 * @date 2020/3/9 15:11
 * @description 任务controller
 */
@Controller
public class TaskController {

    private static final Logger LOG = LoggerFactory.getLogger(TaskController.class);

    @PostMapping("/create")
    @ResponseBody
    public String create(){
        TbTask task = new TbTask();

        boolean insert = task.insert();
        if(insert){
            LOG.info("任务新增成功");
            return "add success";
        }


        return null;
    }

}
