package com.we.dreams.thousand.dto;

/**
 * @author ming
 * @version V1.0
 * @Package com.we.dreams.thousand.dto
 * @date 2020/3/10 11:21
 * @description task dto
 */

public class TaskDto {

    private String user_id;

    private String task_name;

    private String period_start;

    private String period_end;

    private String task_nature;

    private String task_content;

    private String task_href;

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getTask_name() {
        return task_name;
    }

    public void setTask_name(String task_name) {
        this.task_name = task_name;
    }

    public String getPeriod_start() {
        return period_start;
    }

    public void setPeriod_start(String period_start) {
        this.period_start = period_start;
    }

    public String getPeriod_end() {
        return period_end;
    }

    public void setPeriod_end(String period_end) {
        this.period_end = period_end;
    }

    public String getTask_nature() {
        return task_nature;
    }

    public void setTask_nature(String task_nature) {
        this.task_nature = task_nature;
    }

    public String getTask_content() {
        return task_content;
    }

    public void setTask_content(String task_content) {
        this.task_content = task_content;
    }

    public String getTask_href() {
        return task_href;
    }

    public void setTask_href(String task_href) {
        this.task_href = task_href;
    }

    @Override
    public String toString() {
        return "TaskDto{" +
                "task_name='" + task_name + '\'' +
                ", period_start='" + period_start + '\'' +
                ", period_end='" + period_end + '\'' +
                ", task_nature='" + task_nature + '\'' +
                ", task_content='" + task_content + '\'' +
                ", task_href='" + task_href + '\'' +
                '}';
    }
}
