package com.we.dreams.thousand.dto;

import java.io.Serializable;

/**
 * @author ming
 * @version V1.0
 * @Package com.we.dreams.thousand.dto
 * @date 2020/3/10 16:10
 * @description result dto
 */
public class ResultDto<T> implements Serializable {

    private String errorMsg;
    private String errorCode;
    private boolean success;
    private T module;

    //适应前端表格所做的补充字段
    private String code;
    private Integer count;

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public T getModule() {
        return module;
    }

    public void setModule(T module) {
        this.module = module;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}
