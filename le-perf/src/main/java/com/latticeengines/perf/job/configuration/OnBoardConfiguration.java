package com.latticeengines.perf.job.configuration;

import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

public class OnBoardConfiguration {

    LoadConfiguration lc;
    SamplingConfiguration sc;
    DataProfileConfiguration dc;

    public OnBoardConfiguration(LoadConfiguration lc, SamplingConfiguration sc, DataProfileConfiguration dc) {
        this.lc = lc;
        this.sc = sc;
        this.dc = dc;
    }

    public LoadConfiguration getLoadConfiguration() {
        return this.lc;
    }

    public SamplingConfiguration getSamplingConfiguration() {
        return this.sc;
    }

    public DataProfileConfiguration getDataProfileConfiguration() {
        return this.dc;
    }
}
