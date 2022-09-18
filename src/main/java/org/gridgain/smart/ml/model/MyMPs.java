package org.gridgain.smart.ml.model;

import java.io.Serializable;

public class MyMPs implements Serializable {

    private static final long serialVersionUID = -2984794039124053728L;

    // 最大迭代次数
    private Integer maxIterations;
    // 批量大小
    private Integer batchSize;
    // 局部迭代次数
    private Integer locIterations;
    // 随机生成器的种子
    private Long seed;

    public MyMPs(final Integer maxIterations, final Integer batchSize, final Integer locIterations, final Long seed)
    {
        this.maxIterations = maxIterations;
        this.batchSize = batchSize;
        this.locIterations = locIterations;
        this.seed = seed;
    }

    public MyMPs()
    {}

    public Integer getMaxIterations() {
        return maxIterations;
    }

    public void setMaxIterations(Integer maxIterations) {
        this.maxIterations = maxIterations;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public Integer getLocIterations() {
        return locIterations;
    }

    public void setLocIterations(Integer locIterations) {
        this.locIterations = locIterations;
    }

    public Long getSeed() {
        return seed;
    }

    public void setSeed(Long seed) {
        this.seed = seed;
    }
}
