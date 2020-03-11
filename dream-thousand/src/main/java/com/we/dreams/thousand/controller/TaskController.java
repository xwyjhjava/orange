package com.we.dreams.thousand.controller;

import com.dreams.common.LocalDateUtils;
import com.we.dreams.thousand.dto.ResultDto;
import com.we.dreams.thousand.dto.TaskDto;
import com.we.dreams.thousand.mapper.TbTaskMapper;
import com.we.dreams.thousand.model.TbTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

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

    @PostMapping("/create/task")
    @ResponseBody
    public ResultDto create(@RequestBody TaskDto paramTask){

        ResultDto resultDto = new ResultDto();

        TbTask task = new TbTask();
        task.setTaskId(UUID.randomUUID().toString().trim().replaceAll("-", ""));
        task.setUserId(paramTask.getUser_id());
        // 计算两个时间的时长
        long second = LocalDateUtils.getDateTimeDiff(paramTask.getPeriod_start(), paramTask.getPeriod_end(),
                "yyyy-MM-dd HH:mm:ss", "second");
        String hour = String.format("%.2f", second / 3600.0);
        task.setTaskDuration(hour);
        //赋值
        task.setTaskName(paramTask.getTask_name());
        task.setTaskNature(paramTask.getTask_nature());
        task.setTaskContent(paramTask.getTask_content());
        task.setNotehref(paramTask.getTask_href());
        task.setTaskStartTime(LocalDateUtils.getDateTime(paramTask.getPeriod_start(), "yyyy-MM-dd HH:mm:ss"));
        task.setTaskEndTime(LocalDateUtils.getDateTime(paramTask.getPeriod_end(), "yyyy-MM-dd HH:mm:ss"));

        task.setTaskStatus("进行中");
        task.setCreateTime(LocalDateTime.now());

        boolean insert = task.insert();
        if(insert){
            LOG.info("任务新增成功");
            resultDto.setSuccess(true);
        }
        return resultDto;
    }

//    todo 分页查询
    @GetMapping("/list")
    @ResponseBody
    public ResultDto getTaskList(){

        ResultDto resultDto = new ResultDto();

        TbTask task = new TbTask();
        List<TbTask> taskList = task.selectAll();

        LOG.info("list size {}", taskList.size());

        if(taskList.size() > 0){
            resultDto.setSuccess(true);
            resultDto.setModule(taskList);
            resultDto.setCode("0");
            resultDto.setCount(taskList.size());
        }
        return resultDto;
    }

}
