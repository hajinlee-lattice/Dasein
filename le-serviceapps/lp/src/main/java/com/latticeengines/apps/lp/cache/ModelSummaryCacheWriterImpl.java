package com.latticeengines.apps.lp.cache;

import java.util.ArrayList;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.cache.impl.CacheWriterImpl;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.pls.ModelSummary;

@Component("modelSummaryCacheWriter")
public class ModelSummaryCacheWriterImpl extends CacheWriterImpl<ModelSummary> implements ModelSummaryCacheWriter {

    private static final String CACHE_KEY_PREFIX = CacheName.Constants.ModelSummaryCacheName;
    private static final String MODELSUMMARY_ID_LIST_KEY = "MODELSUMMARY_ID_LIST";
    private static final String MODELSUMMARY_ID_KEY = "MODELSUMMARY_ID";
    private static final String MODELSUMMARY_LOCK_KEY = "MODELSUMMARY_LOCK";

    @Override
    protected String getCacheKeyPrefix() {
        return CACHE_KEY_PREFIX;
    }

    @Override
    protected String getIdListKey() {
        return MODELSUMMARY_ID_LIST_KEY;
    }

    @Override
    protected String getObjectKey() {
        return MODELSUMMARY_ID_KEY;
    }

    @Override
    protected String getCacheLockKey() {
        return MODELSUMMARY_LOCK_KEY;
    }

    // prevent hibernate lazy field
    @Override
    protected void fixLazyField(ModelSummary modelSummary) {
        modelSummary.setPredictors(new ArrayList<>());
        modelSummary.setDetails(null);
    }
}
