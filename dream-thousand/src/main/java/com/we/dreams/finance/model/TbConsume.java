package com.we.dreams.finance.model;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.extension.activerecord.Model;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * @author ming
 * @version V1.0
 * @Package com.we.dreams.finance.model
 * @date 2020/6/15 15:26
 * @description 消费类
 */
@TableName("tb_consume")
public class TbConsume extends Model<TbConsume> implements Serializable {

    private static final long serialVersionUID = 1L;

    @TableId(value = "id", type = IdType.AUTO)
    private Integer id;

    private String consumeId;

    //消费金额
    private float ConsumeAmount;

    // 消费时间
    private LocalDateTime consumeDatetime;

    // 消费地点
    private String consumeLocate;

    // 消费内容(消费目的)
    private String consumeContext;

    // 消费类型(衣食住行)
    private String consumeType;

    // 理财收入
    private float incomeOfFinance;

    // 其他收入
    private float incomeOfOthers;

    // 备注
    private String remark;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getConsumeId() {
        return consumeId;
    }

    public void setConsumeId(String consumeId) {
        this.consumeId = consumeId;
    }

    public float getConsumeAmount() {
        return ConsumeAmount;
    }

    public void setConsumeAmount(float consumeAmount) {
        ConsumeAmount = consumeAmount;
    }

    public LocalDateTime getConsumeDatetime() {
        return consumeDatetime;
    }

    public void setConsumeDatetime(LocalDateTime consumeDatetime) {
        this.consumeDatetime = consumeDatetime;
    }

    public String getConsumeLocate() {
        return consumeLocate;
    }

    public void setConsumeLocate(String consumeLocate) {
        this.consumeLocate = consumeLocate;
    }

    public String getConsumeContext() {
        return consumeContext;
    }

    public void setConsumeContext(String consumeContext) {
        this.consumeContext = consumeContext;
    }

    public String getConsumeType() {
        return consumeType;
    }

    public void setConsumeType(String consumeType) {
        this.consumeType = consumeType;
    }

    public float getIncomeOfFinance() {
        return incomeOfFinance;
    }

    public void setIncomeOfFinance(float incomeOfFinance) {
        this.incomeOfFinance = incomeOfFinance;
    }

    public float getIncomeOfOthers() {
        return incomeOfOthers;
    }

    public void setIncomeOfOthers(float incomeOfOthers) {
        this.incomeOfOthers = incomeOfOthers;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }
}
