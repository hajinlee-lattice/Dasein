package com.latticeengines.apps.cdl.service.impl;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.PeriodService;
import com.latticeengines.apps.cdl.service.ZKConfigService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.PeriodBuilderFactory;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.proxy.exposed.objectapi.TransactionProxy;

@Service("periodService")
public class PeriodServiceImpl implements PeriodService {

    private static final Logger log = LoggerFactory.getLogger(PeriodServiceImpl.class);

    @Inject
    private TransactionProxy transactionProxy;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private ZKConfigService zkConfigService;

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
    public String getEvaluationDate() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        String evaluationDate = zkConfigService.getFakeCurrentDate(customerSpace);
        if (StringUtils.isBlank(evaluationDate)) {
            DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE;
            evaluationDate = LocalDate.now().format(formatter);
        } else {
            log.info("Using faked current date for " + customerSpace.getTenantId() + " in ZK: " + evaluationDate);
        }
        return evaluationDate;
    }

    @Override
    public int getMaxPeriodId(String customerSpace, PeriodStrategy periodStrategy) {
        String dateStr = transactionProxy.getMaxTransactionDate(customerSpace,
                dataCollectionService.getActiveVersion(customerSpace));
        LocalDate date = LocalDate.parse(dateStr);
        if (date.isAfter(LocalDate.now())) {
            dateStr = LocalDate.now().toString();
        }
        return PeriodBuilderFactory.build(periodStrategy).toPeriodId(dateStr);
    }

}
