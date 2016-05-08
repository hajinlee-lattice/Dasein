package com.latticeengines.leadprioritization.workflow.steps.pmml;

import org.dmg.pmml.DataField;
import org.dmg.pmml.MiningField;

public class PmmlField {

    public MiningField miningField;
    public DataField dataField;
    
    public PmmlField(MiningField miningField, DataField dataField) {
        this.miningField = miningField;
        this.dataField = dataField;
    }
}
