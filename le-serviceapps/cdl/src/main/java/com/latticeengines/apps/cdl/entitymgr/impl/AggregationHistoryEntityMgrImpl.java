package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.AggregationHistoryEntityMgr;
import com.latticeengines.apps.cdl.repository.jpa.writer.AggregationHistoryWriterRepository;
import com.latticeengines.apps.cdl.repository.reader.AggregationHistoryReaderRepository;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.AggregationHistory;

@Component("aggregationHistoryEntityMgr")
public class AggregationHistoryEntityMgrImpl extends JpaEntityMgrRepositoryImpl<AggregationHistory, Long> implements AggregationHistoryEntityMgr {

    @Inject
    private AggregationHistoryReaderRepository readerRepository;

    @Inject
    private AggregationHistoryWriterRepository writerRepository;

    @Override
    public BaseJpaRepository<AggregationHistory, Long> getRepository() {
        return writerRepository;
    }

}
