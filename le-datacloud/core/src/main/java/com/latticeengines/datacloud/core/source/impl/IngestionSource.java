package com.latticeengines.datacloud.core.source.impl;

import com.latticeengines.datacloud.core.source.Source;

// Ingestion is treated as a special source
public class IngestionSource implements Source {

    private static final long serialVersionUID = 2237469282940403218L;

    private String ingestionName;

    public IngestionSource(String ingestionName) {
        this.ingestionName = ingestionName;
    }

    @Override
    public String getSourceName() {
        return "Ingestion_" + ingestionName;
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
