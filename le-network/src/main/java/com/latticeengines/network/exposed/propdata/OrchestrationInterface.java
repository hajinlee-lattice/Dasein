package com.latticeengines.network.exposed.propdata;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;

public interface OrchestrationInterface {
    public List<OrchestrationProgress> scan(String hdfsPod);
}
