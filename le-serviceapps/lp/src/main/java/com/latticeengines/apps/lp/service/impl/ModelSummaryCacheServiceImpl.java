package com.latticeengines.apps.lp.service.impl;

import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.core.cache.CacheWriter;
import com.latticeengines.apps.core.service.impl.BaseCacheServiceImpl;
import com.latticeengines.apps.lp.cache.ModelSummaryCacheWriter;
import com.latticeengines.apps.lp.entitymgr.ModelSummaryEntityMgr;
import com.latticeengines.apps.lp.service.ModelSummaryCacheService;
import com.latticeengines.domain.exposed.pls.ModelSummary;

@Service("modelSummaryCacheService")
public class ModelSummaryCacheServiceImpl extends BaseCacheServiceImpl<ModelSummary> implements ModelSummaryCacheService {

    @Autowired
    private ModelSummaryCacheWriter modelSummaryCacheWriter;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    protected CacheWriter getCacheWriter() {
        return modelSummaryCacheWriter;
    }

    protected String getThreadPoolName() {
        return "modelsummary-cache-service";
    }

    protected List<ModelSummary> getAll() {
        return modelSummaryEntityMgr.getAll();
    }

    protected List<ModelSummary> findEntitiesByIds(Set<String> ids) {
        return modelSummaryEntityMgr.findModelSummariesByIds(ids);
    }
}
