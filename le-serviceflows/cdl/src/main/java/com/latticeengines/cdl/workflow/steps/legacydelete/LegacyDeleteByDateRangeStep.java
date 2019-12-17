package com.latticeengines.cdl.workflow.steps.legacydelete;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.LegacyDeleteByDateRangeActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete.LegacyDeleteByDateRangeStepConfiguration;
import com.latticeengines.domain.exposed.util.TimeSeriesUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

public class LegacyDeleteByDateRangeStep extends BaseWorkflowStep<LegacyDeleteByDateRangeStepConfiguration> {

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Override
    public void execute() {
        log.info("Start cleanup by date range operation!");
        if(configuration.getCustomerSpace() == null) {
            throw new LedpException(LedpCode.LEDP_40000);
        }
        if(configuration.getEntity() == null || configuration.getEntity() != BusinessEntity.Transaction) {
            throw new LedpException(LedpCode.LEDP_40001);
        }
        if (getAvroDir() == null) {
            log.info("transaction table is empty.");
            return;
        }
        Map<BusinessEntity, Set> actionMap = getMapObjectFromContext(LEGACY_DELTE_BYUOLOAD_ACTIONS,
                BusinessEntity.class, Set.class);
        log.info("actionMap is : {}", JsonUtils.serialize(actionMap));
        if (actionMap != null && actionMap.containsKey(configuration.getEntity())) {
            Set<Action> actionSet = JsonUtils.convertSet(actionMap.get(configuration.getEntity()), Action.class);
            for (Action action : actionSet) {
                LegacyDeleteByDateRangeActionConfiguration actionConfiguration = (LegacyDeleteByDateRangeActionConfiguration) action.getActionConfiguration();
                Date startTime = actionConfiguration.getStartTime();
                Date endTime = actionConfiguration.getEndTime();
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
                String avroDir = getAvroDir();
                Long deletedRows = TimeSeriesUtils.cleanupPeriodData(yarnConfiguration, avroDir, periods, true);
                log.info("delete actionPid is {}, entity is {}, deletedRows is {}.",
                        action.getPid(), configuration.getEntity().name(), deletedRows);
            }
        }
    }

    private String getAvroDir() {
        String customerSpace = configuration.getCustomerSpace().toString();
        Table table = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection
                .ConsolidatedRawTransaction);
        if(table == null) {
            return null;
        }
        if (table.getExtracts() == null || table.getExtracts().size() != 1) {
            return null;
        }
        String avroDir = table.getExtracts().get(0).getPath();
        log.info("avroDir: " + avroDir);
        return avroDir;
    }
}
