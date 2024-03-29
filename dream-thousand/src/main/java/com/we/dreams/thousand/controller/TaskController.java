package com.we.dreams.thousand.controller;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.dreams.common.LocalDateUtils;
import com.we.dreams.thousand.dto.ResultDto;
import com.we.dreams.thousand.dto.TaskDto;
import com.we.dreams.thousand.model.TbTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.time.LocalDateTime;
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
    public ResultDto create(@RequestBody TaskDto paramTask, HttpServletRequest request){

        ResultDto resultDto = new ResultDto();

        TbTask task = new TbTask();
        task.setTaskId(UUID.randomUUID().toString().trim().replaceAll("-", ""));

        // 从Session中取用户ID
        String userId = request.getSession().getAttribute("userId").toString();
        task.setUserId(userId);

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

        task.setTaskStatus("完成");
        task.setCreateTime(LocalDateTime.now());

        boolean insert = task.insert();
        if(insert){
            LOG.info("任务新增成功");
            resultDto.setSuccess(true);
        }
        return resultDto;
    }

    @GetMapping("/list")
    @ResponseBody
    public ResultDto getTaskList(@RequestParam("pageNum") Integer pageNum, @RequestParam("pageSize") Integer pageSize){

        ResultDto resultDto = new ResultDto();

        TbTask task = new TbTask();
        Page<TbTask> page = new Page<>(pageNum, pageSize);
        IPage<TbTask> taskList = task.selectPage(page, null);

        long size = taskList.getSize();
        LOG.info("list size {}", size);

        if(size > 0){
            resultDto.setSuccess(true);
            resultDto.setModule(taskList.getRecords());
            resultDto.setCode(0);
            resultDto.setCount(taskList.getTotal());
        }
        return resultDto;
    }

}
