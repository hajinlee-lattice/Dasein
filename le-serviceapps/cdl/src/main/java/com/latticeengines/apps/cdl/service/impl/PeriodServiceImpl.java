package com.latticeengines.apps.cdl.service.impl;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.provision.impl.CDLComponent;
import com.latticeengines.apps.cdl.service.BusinessCalendarService;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.PeriodService;
import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.PeriodBuilderFactory;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;
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

    @Inject
    private BusinessCalendarService businessCalendarService;

    @Override
    public List<String> getPeriodNames() {
        return Arrays.asList( //
                PeriodStrategy.Template.Week.name(), //
                PeriodStrategy.Template.Month.name(), //
                PeriodStrategy.Template.Quarter.name(), //
                PeriodStrategy.Template.Year.name() //
        );
    }

    @Override
    public List<PeriodStrategy> getPeriodStrategies() {
        BusinessCalendar calendar = businessCalendarService.find();
        if (calendar != null) {
            return Arrays.asList(new PeriodStrategy(calendar, PeriodStrategy.Template.Week),
                    new PeriodStrategy(calendar, PeriodStrategy.Template.Month),
                    new PeriodStrategy(calendar, PeriodStrategy.Template.Quarter),
                    new PeriodStrategy(calendar, PeriodStrategy.Template.Year));
        } else {
            return PeriodStrategy.NATURAL_PERIODS;
        }
    }

    @Override
    public String getEvaluationDate() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        String evaluationDate = zkConfigService.getFakeCurrentDate(customerSpace, CDLComponent.componentName);
        if (StringUtils.isBlank(evaluationDate)) {
            DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE;
            evaluationDate = LocalDate.now().format(formatter);
        } else {
            log.info("Using faked current date for " + customerSpace.getTenantId() + " in ZK: " + evaluationDate);
        }
        return evaluationDate;
    }

    @Override
    public Map<PeriodStrategy.Template, Integer> getPeriodId(String date, PeriodStrategy periodStrategy) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (StringUtils.isBlank(date)) {
            DataCollection.Version version = dataCollectionService.getActiveVersion(customerSpace.toString());
            date = getMaxTxnDate(customerSpace.toString(), periodStrategy, version);
        }
        if (StringUtils.isBlank(date)) {
            throw new RuntimeException(
                    String.format("Tenant %s does not have max transaction date saved. Please provide date.",
                            customerSpace.getTenantId()));
        }

        List<PeriodStrategy> strategies = new ArrayList<>();
        if (periodStrategy != null) {
            strategies.add(periodStrategy);
        } else {
            strategies = getPeriodStrategies();
        }

        Map<PeriodStrategy.Template, Integer> ids = new HashMap<>();
        for (PeriodStrategy strategy : strategies) {
            ids.put(strategy.getTemplate(), PeriodBuilderFactory.build(periodStrategy).toPeriodId(date));
        }
        return ids;
    }

    @Override
    public int getMaxPeriodId(String customerSpace, PeriodStrategy periodStrategy, DataCollection.Version version) {
        String dateStr = getMaxTxnDate(customerSpace, periodStrategy, version);
        return PeriodBuilderFactory.build(periodStrategy).toPeriodId(dateStr);
    }

    @Override
    public PeriodStrategy getApsRollupPeriod(DataCollection.Version version) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        DataCollectionStatus dataCollectionStatus = dataCollectionService.getOrCreateDataCollectionStatus(customerSpace.toString(), version);
        String rollingPeriod = dataCollectionStatus.getApsRollingPeriod();
        final String finalPeriod = StringUtils.isBlank(rollingPeriod) ? "Month" : rollingPeriod;
        return getPeriodStrategies().stream().filter(x -> finalPeriod.equals(x.getName())).findFirst().orElse(null);
    }

    private String getMaxTxnDate(String customerSpace, PeriodStrategy periodStrategy, DataCollection.Version version) {
        if (version == null) {
            version = dataCollectionService.getActiveVersion(customerSpace);
        }
        DataCollectionStatus dataCollectionStatus = dataCollectionService.getOrCreateDataCollectionStatus(customerSpace, version);
        int maxTxnDate = dataCollectionStatus == null ? -1 : dataCollectionStatus.getMaxTxnDate();
        String dateStr;
        if (maxTxnDate < 41633) { // before 2000-JAN-01
            dateStr = transactionProxy.getMaxTransactionDate(customerSpace, version);
            LocalDate date = LocalDate.parse(dateStr);
            if (date.isAfter(LocalDate.now())) {
                dateStr = LocalDate.now().toString();
            }
        } else {
            dateStr = DateTimeUtils.dayPeriodToDate(maxTxnDate);
        }
        return dateStr;
    }

}
