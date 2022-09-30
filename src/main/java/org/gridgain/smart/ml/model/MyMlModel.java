package org.gridgain.smart.ml.model;

import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.preprocessing.Preprocessor;

import java.io.Serializable;

public class MyMlModel implements Serializable {
    private static final long serialVersionUID = -6744097552234047490L;

    private IgniteModel model;
    private Preprocessor preprocessor;
    private String score;

    public MyMlModel()
    {}

    public MyMlModel(IgniteModel model)
    {
        this.model = model;
    }

    public MyMlModel(IgniteModel model, String score)
    {
        this.model = model;
        this.score = score;
    }

    public MyMlModel(IgniteModel model, Preprocessor preprocessor)
    {
        this.model = model;
        this.preprocessor = preprocessor;
    }

    public MyMlModel(IgniteModel model, Preprocessor preprocessor, String score)
    {
        this.model = model;
        this.preprocessor = preprocessor;
        this.score = score;
    }

    public IgniteModel getModel() {
        return model;
    }

    public void setModel(IgniteModel model) {
        this.model = model;
    }

    public Preprocessor getPreprocessor() {
        return preprocessor;
    }

    public void setPreprocessor(Preprocessor preprocessor) {
        this.preprocessor = preprocessor;
    }

    public String getScore() {
        return score;
    }

    public void setScore(String score) {
        this.score = score;
    }
}










































