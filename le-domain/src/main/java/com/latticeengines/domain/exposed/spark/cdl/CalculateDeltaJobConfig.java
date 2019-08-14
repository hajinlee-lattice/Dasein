package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class CalculateDeltaJobConfig extends SparkJobConfig {
    public static final String NAME = "calculateDelta";

    private DataUnit currentAccountUniverse;
    private DataUnit currentContactUniverse;
    private DataUnit previousAccountUniverse;
    private DataUnit previousContactUniverse;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return 6;
    }

    @JsonProperty("CurrentAccountUniverse")
    public DataUnit getCurrentAccountUniverse() {
        return currentAccountUniverse;
    }

    public void setCurrentAccountUniverse(DataUnit currentAccountUniverse) {
        this.currentAccountUniverse = currentAccountUniverse;
    }

    @JsonProperty("CurrentContactUniverse")
    public DataUnit getCurrentContactUniverse() {
        return currentContactUniverse;
    }

    public void setCurrentContactUniverse(DataUnit currentContactUniverse) {
        this.currentContactUniverse = currentContactUniverse;
    }

    @JsonProperty("PreviousAccountUniverse")
    public DataUnit getPreviousAccountUniverse() {
        return previousAccountUniverse;
    }

    public void setPreviousAccountUniverse(DataUnit previousAccountUniverse) {
        this.previousAccountUniverse = previousAccountUniverse;
    }

    @JsonProperty("PreviousContactUniverse")
    public DataUnit getPreviousContactUniverse() {
        return previousContactUniverse;
    }

    public void setPreviousContactUniverse(DataUnit previousContactUniverse) {
        this.previousContactUniverse = previousContactUniverse;
    }

}
