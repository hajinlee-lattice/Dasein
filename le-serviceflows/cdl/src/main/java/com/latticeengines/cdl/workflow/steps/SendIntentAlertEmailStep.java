package com.latticeengines.cdl.workflow.steps;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.service.GlobalAuthSubscriptionService;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.IntentAlertEmailInfo;
import com.latticeengines.domain.exposed.cdl.activity.ActivityBookkeeping;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.SendIntentAlertEmailStepConfiguration;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Component("sendIntentAlertEmailStep")
public class SendIntentAlertEmailStep extends BaseWorkflowStep<SendIntentAlertEmailStepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(SendIntentAlertEmailStep.class);

    @Inject
    private ActivityStoreProxy activityStoreProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    EmailService emailService;

    @Inject
    GlobalAuthSubscriptionService subscriptionService;

    @Inject
    private TenantService tenantService;

    private String subject;

    private CustomerSpace customerSpace;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        // send email
        Map<String, Object> params = getEmailInfo();
        List<String> recipients = subscriptionService.getEmailsByTenantId(customerSpace.toString());
        Tenant tenant = tenantService.findByTenantId(customerSpace.toString());
        emailService.sendDnbIntentAlertEmail(tenant, recipients, subject, params);
        // Set IntentAlertVersion in data collection status table
        String intentAlertVersion = updateIntentAlertVersion();
        log.info("Done with sending intent alert email, update intent alert version to {}", intentAlertVersion);
    }

    private Map<String, Object> getEmailInfo() {
        Table newAccountsTable = getObjectFromContext(INTENT_ALERT_NEW_ACCOUNT_TABLE_NAME, Table.class);
        int numBuyIntents = 0;
        int numResearchIntents = 0;
        Map<String, List<IntentAlertEmailInfo.Intent>> modelMap = new HashMap<>();
        Map<String, IntentAlertEmailInfo.TopItem> industryCountMap = new HashMap<>();
        Map<String, IntentAlertEmailInfo.TopItem> locationCountMap = new HashMap<>();
        try {
            List<Extract> extracts = newAccountsTable.getExtracts();
            for (Extract extract : extracts) {
                List<String> avroFiles = HdfsUtils.getFilesByGlob(yarnConfiguration, extract.getPath());
                for (String avroFile : avroFiles) {
                    FileReader<GenericRecord> reader = AvroUtils.getAvroFileReader(yarnConfiguration,
                            new Path(avroFile));
                    for (GenericRecord record : reader) {
                        IntentAlertEmailInfo.Intent intentItem = new IntentAlertEmailInfo.Intent(record);
                        addToModelMap(modelMap, intentItem);
                        addToCountMap(industryCountMap, intentItem.getIndustry());
                        addToCountMap(locationCountMap, intentItem.getLocation());
                        if (stageEqual(intentItem, IntentAlertEmailInfo.StageType.BUY)) {
                            numBuyIntents++;
                            industryCountMap.get(intentItem.getIndustry()).increaseNum_buy();
                            locationCountMap.get(intentItem.getLocation()).increaseNum_buy();
                        } else if (stageEqual(intentItem, IntentAlertEmailInfo.StageType.RESEARCH)) {
                            numResearchIntents++;
                            industryCountMap.get(intentItem.getIndustry()).increaseNum_research();
                            locationCountMap.get(intentItem.getLocation()).increaseNum_research();
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        Map<String, Object> params = new HashMap<>();
        String[] dateRange = getDateRange();
        subject = String.format("Dun & Bradstreet's Intent Alert Report - {} to {}", dateRange[0], dateRange[1]);
        params.put("date_range", String.format("{} - {}", dateRange[0], dateRange[1]));
        params.put("num_buy_intents", numBuyIntents);
        params.put("num_research_intents", numResearchIntents);
        params.put("summary_text", getSummaryText(numBuyIntents, numResearchIntents));
        params.put("top_industries", getTopListFromMap(industryCountMap));
        params.put("top_locations", getTopListFromMap(locationCountMap));
        params.put("model_pairs", getModelPairFromMap(modelMap));
        params.put("attachment", getAttachment());
        return params;
    }

    private byte[] getAttachment() {
        try {
            Table allAccountsTable = getObjectFromContext(INTENT_ALERT_ALL_ACCOUNT_TABLE_NAME, Table.class);
            String filePath = allAccountsTable.getExtracts().get(0).getPath();
            if (HdfsUtils.fileExists(yarnConfiguration, filePath)) {
                return IOUtils.toByteArray(HdfsUtils.getInputStream(yarnConfiguration, filePath));
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    private String[] getDateRange() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MM/dd/yyyy");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.setFirstDayOfWeek(Calendar.MONDAY);
        calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        String startTime = simpleDateFormat.format(calendar.getTime());
        calendar.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
        String endTime = simpleDateFormat.format(calendar.getTime());
        return new String[] { startTime, endTime };
    }

    private String getSummaryText(int numBuyIntents, int numResearchIntents) {
        if (numBuyIntents >= numResearchIntents) {
            return "You have companies with high buying intent this week.";
        } else {
            return "You have companies with high researching intent this week.";
        }
    }

    private void addToCountMap(Map<String, IntentAlertEmailInfo.TopItem> map, String name) {
        IntentAlertEmailInfo.TopItem topItem = map.get(name);
        if (topItem == null) {
            topItem = new IntentAlertEmailInfo.TopItem();
            topItem.setName(name);
            map.put(name, topItem);
        }
        topItem.increaseNum_intents();
    }

    private void addToModelMap(Map<String, List<IntentAlertEmailInfo.Intent>> map,
            IntentAlertEmailInfo.Intent subscriptionItem) {
        List<IntentAlertEmailInfo.Intent> list = map.get(subscriptionItem.getModel());
        if (list == null) {
            list = new ArrayList<>();
            map.put(subscriptionItem.getModel(), list);
        }
        list.add(subscriptionItem);
    }

    private List<IntentAlertEmailInfo.TopItem> getTopListFromMap(Map<String, IntentAlertEmailInfo.TopItem> map) {
        List<IntentAlertEmailInfo.TopItem> topList = map.values().stream().collect(Collectors.toList());
        topList.sort(Comparator.comparing(IntentAlertEmailInfo.TopItem::getNum_intents).reversed());
        topList = topList.size() > IntentAlertEmailInfo.TOPLIMIT ? topList.subList(0, IntentAlertEmailInfo.TOPLIMIT)
                : topList;
        updatePercentage(topList);
        return topList;
    }

    private List<List<HashMap<String, Object>>> getModelPairFromMap(
            Map<String, List<IntentAlertEmailInfo.Intent>> map) {
        List<List<HashMap<String, Object>>> pairList = new ArrayList<>();
        int index = 0;
        List<HashMap<String, Object>> pair = new ArrayList<>();
        for (String key : map.keySet()) {
            if ((index++ & 1) == 0) {
                pair = new ArrayList<>();
                pairList.add(pair);
            }
            HashMap<String, Object> intentMap = new HashMap<>();
            intentMap.put("name", key);
            intentMap.put("intents", map.get(key));
            pair.add(intentMap);
        }
        return pairList;
    }

    private void updatePercentage(List<IntentAlertEmailInfo.TopItem> list) {
        double max = Collections.max(list, Comparator.comparing(IntentAlertEmailInfo.TopItem::getNum_intents))
                .getNum_intents();
        for (IntentAlertEmailInfo.TopItem item : list) {
            item.setBuy_percentage(item.getNum_buy() / max);
            item.setResearch_percentage(item.getNum_research() / max);
        }
    }

    private boolean stageEqual(IntentAlertEmailInfo.Intent intent, IntentAlertEmailInfo.StageType type) {
        return intent.getStage().equalsIgnoreCase(type.toString());
    }

    private String updateIntentAlertVersion() {
        DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(customerSpace.toString());
        List<AtlasStream> streams = activityStoreProxy.getStreams(customerSpace.toString());
        AtlasStream intentStream = streams.stream()
                .filter(stream -> (stream.getStreamType() == AtlasStream.StreamType.DnbIntentData)).findFirst().get();
        String streamId = intentStream.getStreamId();
        DataCollectionStatus dcStatus = dataCollectionProxy.getOrCreateDataCollectionStatus(customerSpace.toString(),
                activeVersion);
        // Use last intent import time as intent alert version
        ActivityBookkeeping bookkeeping = dcStatus.getActivityBookkeeping();
        Map<Integer, Long> records = bookkeeping.streamRecord.get(streamId);
        Integer dateId = records.keySet().stream().sorted(Comparator.reverseOrder()).findFirst().get();
        String intentAlertVersion = String.valueOf(dateId);
        dcStatus.setIntentAlertVersion(intentAlertVersion);
        // Write back to Data Collection Status tables
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(customerSpace.toString(), dcStatus, activeVersion);
        log.info("New intent alert version is {}", intentAlertVersion);
        return intentAlertVersion;
    }
}
