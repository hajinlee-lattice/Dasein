package com.latticeengines.apps.cdl.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.AggregationHistoryEntityMgr;
import com.latticeengines.apps.cdl.service.AggregationHistoryService;
import com.latticeengines.domain.exposed.cdl.AggregationHistory;

@Component("aggregationHistoryService")
public class AggregationHistoryServiceImpl implements AggregationHistoryService {

    private static final Logger log = LoggerFactory.getLogger(AggregationHistoryServiceImpl.class);

    @Inject
    private AggregationHistoryEntityMgr aggregationHistoryEntityMgr;

    @Override
    public AggregationHistory create(AggregationHistory aggregationHistory) {
        aggregationHistoryEntityMgr.create(aggregationHistory);
        return aggregationHistory;
    }
}
