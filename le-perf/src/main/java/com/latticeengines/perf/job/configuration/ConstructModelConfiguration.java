package com.latticeengines.perf.job.configuration;

import com.latticeengines.domain.exposed.modeling.Model;

public class ConstructModelConfiguration {
    OnBoardConfiguration obc;

    Model model;

    public ConstructModelConfiguration(OnBoardConfiguration obc, Model model) {
        this.obc = obc;
        this.model = model;
    }

    public OnBoardConfiguration getOnBoardConfiguration() {
        return this.obc;
    }

    public Model getModel() {
        return this.model;
    }
}
