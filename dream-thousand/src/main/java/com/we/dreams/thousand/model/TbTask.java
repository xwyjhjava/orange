package com.we.dreams.thousand.model;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.extension.activerecord.Model;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * @author ming
 * @version V1.0
 * @Package com.we.dreams.thousand.model
 * @date 2020/3/9 13:28
 * @description 任务表
 */
@TableName("tb_task")
public class TbTask extends Model<TbTask> implements Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    protected Serializable pkVal() {
        return id;
    }

    @TableId(value = "id", type = IdType.AUTO)
    private Integer id;

    private String taskId;

    private String  userId;

    private String taskName;

    private LocalDateTime taskStartTime;

    private LocalDateTime taskEndTime;

    private String taskDuration;

    private String taskNature;

    private String taskContent;

    private String notehref;

    private String taskStatus;

    private LocalDateTime createTime;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public LocalDateTime getTaskStartTime() {
        return taskStartTime;
    }

    public void setTaskStartTime(LocalDateTime taskStartTime) {
        this.taskStartTime = taskStartTime;
    }

    public LocalDateTime getTaskEndTime() {
        return taskEndTime;
    }

    public void setTaskEndTime(LocalDateTime taskEndTime) {
        this.taskEndTime = taskEndTime;
    }

    public String getTaskDuration() {
        return taskDuration;
    }

    public void setTaskDuration(String taskDuration) {
        this.taskDuration = taskDuration;
    }

    public String getTaskNature() {
        return taskNature;
    }

    public void setTaskNature(String taskNature) {
        this.taskNature = taskNature;
    }

    public String getTaskContent() {
        return taskContent;
    }

    public void setTaskContent(String taskContent) {
        this.taskContent = taskContent;
    }

    public String getNotehref() {
        return notehref;
    }

    public void setNotehref(String notehref) {
        this.notehref = notehref;
    }

    public String getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(String taskStatus) {
        this.taskStatus = taskStatus;
    }

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }
}
