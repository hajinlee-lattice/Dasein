package com.latticeengines.perf.job.configuration;

import com.latticeengines.domain.exposed.dataplatform.DataProfileConfiguration;
import com.latticeengines.domain.exposed.dataplatform.LoadConfiguration;
import com.latticeengines.domain.exposed.dataplatform.SamplingConfiguration;

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
