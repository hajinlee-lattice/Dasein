package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.service.PeriodService;
import com.latticeengines.domain.exposed.cdl.PeriodBuilderFactory;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.proxy.exposed.objectapi.TransactionProxy;

@Service("periodService")
public class PeriodServiceImpl implements PeriodService {

    @Autowired
    private TransactionProxy transactionProxy;

    @Override
    public List<String> getPeriodNames() {
        return Arrays.asList( //
                TimeFilter.Period.Week.name(), //
                TimeFilter.Period.Month.name(), //
                TimeFilter.Period.Quarter.name(), //
                TimeFilter.Period.Year.name() //
        );
    }

    @Override
    public List<PeriodStrategy> getPeriodStrategies() {
        return PeriodStrategy.NATURAL_PERIODS;
    }

    @Override
    public int getEvaluationPeriod(String customerSpace, DataCollection.Version version,
            PeriodStrategy periodStrategy) {
        return getMaxPeriod(customerSpace, version, periodStrategy) - 1;
    }

    @Override
    public int getMaxPeriod(String customerSpace, DataCollection.Version version, PeriodStrategy periodStrategy) {
        String dateStr = transactionProxy.getMaxTransactionDate(customerSpace, version);
        return PeriodBuilderFactory.build(periodStrategy).toPeriodId(dateStr);
    }

}
