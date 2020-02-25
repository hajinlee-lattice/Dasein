package com.latticeengines.cdl.workflow.steps.legacydelete;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.LegacyDeleteByDateRangeActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete.LegacyDeleteByDateRangeStepConfiguration;
import com.latticeengines.domain.exposed.util.TimeSeriesUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.util.TableCloneUtils;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;
import com.latticeengines.yarn.exposed.service.EMREnvService;

@Component(LegacyDeleteByDateRangeStep.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class LegacyDeleteByDateRangeStep extends BaseWorkflowStep<LegacyDeleteByDateRangeStepConfiguration> {

    static final String BEAN_NAME = "legacyDeleteByDateRangeStep";

    private static Logger log = LoggerFactory.getLogger(LegacyDeleteByDateRangeStep.class);

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private EMREnvService emrEnvService;

    private DataCollection.Version inactive;
    private DataCollection.Version active;

    @Override
    public void execute() {
        log.info("Start cleanup by date range operation!");
        if (configuration.getCustomerSpace() == null) {
            throw new LedpException(LedpCode.LEDP_40000);
        }
        if (configuration.getEntity() == null || configuration.getEntity() != BusinessEntity.Transaction) {
            throw new LedpException(LedpCode.LEDP_40001);
        }
        Map<BusinessEntity, Set> actionMap = getMapObjectFromContext(LEGACY_DELETE_BYDATERANGE_ACTIONS,
                BusinessEntity.class, Set.class);
        log.info("actionMap is : {}", JsonUtils.serialize(actionMap));
        if (actionMap == null || !actionMap.containsKey(configuration.getEntity())) {
            return;
        }
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION,
                DataCollection.Version.class);
        active = getObjectFromContext(CDL_ACTIVE_VERSION,
                DataCollection.Version.class);
        if (getAvroDir() == null) {
            log.info("transaction table is empty.");
            if (noImport()) {
                log.error("cannot clean up transaction table with no import.");
                throw new IllegalStateException("cannot clean up transaction table with no import, PA failed");
            }
            return;
        }
        Set<Action> actionSet = JsonUtils.convertSet(actionMap.get(configuration.getEntity()), Action.class);
        for (Action action : actionSet) {
            LegacyDeleteByDateRangeActionConfiguration actionConfiguration = (LegacyDeleteByDateRangeActionConfiguration) action.getActionConfiguration();
            Date startTime = actionConfiguration.getStartTime();
            Date endTime = actionConfiguration.getEndTime();
            if (startTime == null || endTime == null) {
                throw new LedpException(LedpCode.LEDP_40002);
            }

            if (startTime.getTime() > endTime.getTime()) {
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
                throw new LedpException(LedpCode.LEDP_40004, new String[]{pe.getMessage()});
            }
            String avroDir = getAvroDir();
            Long deletedRows = TimeSeriesUtils.cleanupPeriodData(yarnConfiguration, avroDir, periods, true);
            log.info("delete actionPid is {}, entity is {}, deletedRows is {}.",
                    action.getPid(), configuration.getEntity().name(), deletedRows);
        }
    }

    private String getAvroDir() {
        String customerSpace = configuration.getCustomerSpace().toString();
        Table table = dataCollectionProxy.getTable(customerSpace, //
                TableRoleInCollection.ConsolidatedRawTransaction, inactive);
        if (table == null) {
            clonePeriodStore(TableRoleInCollection.ConsolidatedRawTransaction, customerSpace);
            table = dataCollectionProxy.getTable(customerSpace, //
                    TableRoleInCollection.ConsolidatedRawTransaction, inactive);
        }
        if (table == null) {
            return null;
        }
        if (table.getExtracts() == null || table.getExtracts().size() != 1) {
            return null;
        }
        String avroDir = table.getExtracts().get(0).getPath();
        log.info("avroDir: " + avroDir);
        return avroDir;
    }

    private boolean noImport() {
        Map<BusinessEntity, List> entityImportsMap = getMapObjectFromContext(CONSOLIDATE_INPUT_IMPORTS,
                BusinessEntity.class, List.class);
        return MapUtils.isEmpty(entityImportsMap) || !entityImportsMap.containsKey(configuration.getEntity());
    }

    private void clonePeriodStore(TableRoleInCollection role, String customerSpace) {
        Table activeTable = dataCollectionProxy.getTable(customerSpace, role, active);
        String cloneName = NamingUtils.timestamp(role.name());
        String queue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        queue = LedpQueueAssigner.overwriteQueueAssignment(queue, emrEnvService.getYarnQueueScheme());
        Table inactiveTable = TableCloneUtils //
                .cloneDataTable(yarnConfiguration, CustomerSpace.parse(customerSpace), cloneName, activeTable, queue);
        metadataProxy.createTable(customerSpace, cloneName, inactiveTable);
        dataCollectionProxy.upsertTable(customerSpace, cloneName, role, inactive);
    }
}
