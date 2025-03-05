package com.ybt.seaotter.common.pojo;

public class JobCallbackMessage {
    /**
     * 业务侧jobId
     */
    private String jobId;

    /**
     * 任务状态
     */
    // 1: 任务开始, 2: 运行中, 3: 成功, -1: 失败
    private Integer state;
    /**
     * 任务总记录数
     */
    private Long totalRecords;
    /**
     * 任务已处理记录数
     */
    private Long handledRecords;
    /**
     * 任务失败原因
     */
    private String message;

    public JobCallbackMessage(String jobId) {
        this.jobId = jobId;
        this.state = 0;
    }

    public JobCallbackMessage(String jobId, Integer state, Long totalRecords, Long handledRecords) {
        this.jobId = jobId;
        this.state = state;
        this.totalRecords = totalRecords;
        this.handledRecords = handledRecords;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public Integer getState() {
        return state;
    }

    public void setState(Integer state) {
        this.state = state;
    }

    public Long getTotalRecords() {
        return totalRecords;
    }

    public void setTotalRecords(Long totalRecords) {
        this.totalRecords = totalRecords;
    }

    public Long getHandledRecords() {
        return handledRecords;
    }

    public void setHandledRecords(Long handledRecords) {
        this.handledRecords = handledRecords;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
