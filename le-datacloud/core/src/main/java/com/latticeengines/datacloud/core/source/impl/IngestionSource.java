package com.latticeengines.datacloud.core.source.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;

// Place holder of Ingestion
@Component("ingestionSource")
public class IngestionSource implements Source {

    private static final long serialVersionUID = 2237469282940403218L;

    private String ingestionName;

    public IngestionSource() {

    }

    public IngestionSource(String ingestionName) {
        this.ingestionName = ingestionName;
    }

    @Override
    public String getSourceName() {
        return "IngestionSource";
    }

    @Override
    public String getTimestampField() {
        return null;
    }

    @Override
    public String[] getPrimaryKey() {
        return null;
    }

    @Override
    public String getDefaultCronExpression() {
        return null;
    }

    public String getIngestionName() {
        return ingestionName;
    }

    public void setIngestionName(String ingestionName) {
        this.ingestionName = ingestionName;
    }

}
