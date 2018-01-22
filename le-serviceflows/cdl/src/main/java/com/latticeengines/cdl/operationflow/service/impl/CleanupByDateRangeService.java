package com.latticeengines.cdl.operationflow.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.cdl.operationflow.service.MaintenanceOperationService;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.domain.exposed.cdl.CleanupByDateRangeConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.TimeSeriesUtils;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;

@Component("cleanupByDateRangeService")
public class CleanupByDateRangeService extends MaintenanceOperationService<CleanupByDateRangeConfiguration> {

    private static Logger log = LoggerFactory.getLogger(CleanupByDateRangeService.class);

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private YarnConfiguration yarnConfiguration;

    @Override
    public Map<String, Long> invoke(CleanupByDateRangeConfiguration config) {
        log.info("Start cleanup by date range operation!");
        Map<String, Long> report = new HashMap<>();
        if(config.getCustomerSpace() == null) {
            throw new LedpException(LedpCode.LEDP_40000);
        }

        if(config.getEntity() == null || config.getEntity() != BusinessEntity.Transaction) {
            throw new LedpException(LedpCode.LEDP_40001);
        }

        Date startTime = config.getStartTime();
        Date endTime = config.getEndTime();
        if(startTime == null || endTime == null) {
            throw new LedpException(LedpCode.LEDP_40002);
        }

        if(startTime.getTime() > endTime.getTime()) {
            throw new LedpException(LedpCode.LEDP_40003);
        }

        Set<Integer> periods = new HashSet<Integer>();
        try {
            Calendar calendar = Calendar.getInstance();
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            calendar.setTime(simpleDateFormat.parse(simpleDateFormat.format(startTime)));

            while (calendar.getTime().compareTo(endTime) <= 0) {
                log.info("Time: " + calendar.getTime());

                Integer period = DateTimeUtils.dateToDayPeriod(simpleDateFormat.format(calendar.getTime()));
                log.info("Period: " + period);

                periods.add(period);
                calendar.add(Calendar.DAY_OF_YEAR, 1);
            }
        } catch (ParseException pe) {
            throw new LedpException(LedpCode.LEDP_40004, new String[] { pe.getMessage() });
        }

        Table table = dataCollectionProxy.getTable(config.getCustomerSpace(), TableRoleInCollection
                .ConsolidatedRawTransaction);
        if(table == null) {
            throw new LedpException(LedpCode.LEDP_40005, new String[] { config.getCustomerSpace() });
        }
        if (table.getExtracts() == null || table.getExtracts().size() != 1) {
            throw new LedpException(LedpCode.LEDP_40006, new String[] { config.getCustomerSpace() });
        }

        String avroDir = table.getExtracts().get(0).getPath();
        log.info("avroDir: " + avroDir);

        Long deletedRows = TimeSeriesUtils.cleanupPeriodData(yarnConfiguration, avroDir, periods, true);
        report.put(config.getEntity().name(), deletedRows);
        return report;
    }

    @VisibleForTesting
    public void setDataCollectionProxy(DataCollectionProxy dataCollectionProxy) {
        this.dataCollectionProxy = dataCollectionProxy;
    }
}
