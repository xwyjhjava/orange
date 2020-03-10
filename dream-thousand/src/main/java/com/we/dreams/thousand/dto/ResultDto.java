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
}
