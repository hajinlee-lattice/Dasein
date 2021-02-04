package com.latticeengines.cdl.workflow.steps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeConstants;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.service.GlobalAuthSubscriptionService;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
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

    private int numBuyIntents;

    private int numResearchIntents;

    private LocalDate startDay;

    private LocalDate endDay;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        Map<String, Object> params = getEmailParams();
        if (MapUtils.isNotEmpty(params)) {
            List<String> recipients = subscriptionService.getEmailsByTenantId(customerSpace.toString());
            if (CollectionUtils.isEmpty(recipients)) {
                return;
            }
            Tenant tenant = tenantService.findByTenantId(customerSpace.toString());
            emailService.sendDnbIntentAlertEmail(tenant, recipients, subject, params);
        } else {
            log.info("No intent alert email sent for customer {}.", customerSpace.toString());
        }
        // Set IntentAlertVersion in data collection status table
        String intentAlertVersion = updateIntentAlertVersion();
        log.info("Done with sending intent alert email, update intent alert version to {}", intentAlertVersion);
    }

    private Map<String, Object> getEmailParams() {
        Map<String, List<IntentAlertEmailInfo.Intent>> modelMap = new HashMap<>();
        Map<String, IntentAlertEmailInfo.TopItem> industryCountMap = new HashMap<>();
        Map<String, IntentAlertEmailInfo.TopItem> locationCountMap = new HashMap<>();
        numBuyIntents = 0;
        numResearchIntents = 0;
        LocalDate now = new LocalDate();
        startDay = now.withDayOfWeek(DateTimeConstants.MONDAY);
        endDay = now.withDayOfWeek(DateTimeConstants.SUNDAY);
        generateEmailInfo(modelMap, industryCountMap, locationCountMap);

        Map<String, Object> params = new HashMap<>();
        subject = String.format("Dun & Bradstreet's Intent Alert Report - %s to %s", startDay.toString("MM/dd/yyyy"),
                endDay.toString("MM/dd/yyyy"));
        params.put("date_range",
                String.format("%s - %s", startDay.toString("MM/dd/yyyy"), endDay.toString("MM/dd/yyyy")));
        params.put("num_buy_intents", numBuyIntents);
        params.put("num_research_intents", numResearchIntents);
        params.put("summary_text", getSummaryText(numBuyIntents, numResearchIntents));
        params.put("top_industries", getTopListFromMap(industryCountMap));
        params.put("top_locations", getTopListFromMap(locationCountMap));
        params.put("model_pairs", getModelPairFromMap(modelMap));
        byte[] attachment = getAttachment();
        params.put("attachment", attachment);
        params.put("attachment_name",
                String.format("intent_data_%s_%s.csv", startDay.toString("yyyyMMdd"), endDay.toString("yyyyMMdd")));
        if (MapUtils.isEmpty(modelMap) && attachment == null) {
            return null;
        }
        return params;
    }

    private void generateEmailInfo(Map<String, List<IntentAlertEmailInfo.Intent>> modelMap,
            Map<String, IntentAlertEmailInfo.TopItem> industryCountMap,
            Map<String, IntentAlertEmailInfo.TopItem> locationCountMap) {
        String newAccountsTableName = getStringValueFromContext(INTENT_ALERT_NEW_ACCOUNT_TABLE_NAME);
        Table newAccountsTable = metadataProxy.getTableSummary(customerSpace.toString(), newAccountsTableName);
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
                            if (industryCountMap.get(intentItem.getIndustry()) != null) {
                                industryCountMap.get(intentItem.getIndustry()).increaseNumBuy();
                            }
                            if (locationCountMap.get(intentItem.getLocation()) != null) {
                                locationCountMap.get(intentItem.getLocation()).increaseNumBuy();
                            }
                        } else if (stageEqual(intentItem, IntentAlertEmailInfo.StageType.RESEARCH)) {
                            numResearchIntents++;
                            if (industryCountMap.get(intentItem.getIndustry()) != null) {
                                industryCountMap.get(intentItem.getIndustry()).increaseNumResearch();
                            }
                            if (locationCountMap.get(intentItem.getLocation()) != null) {
                                locationCountMap.get(intentItem.getLocation()).increaseNumResearch();
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private byte[] getAttachment() {
        try {
            String attachmentPath = StringUtils
                    .appendIfMissing(getStringValueFromContext(INTENT_ALERT_ALL_ACCOUNT_TABLE_NAME), "/*.csv");
            List<String> attachments = HdfsUtils.getFilesByGlob(yarnConfiguration, attachmentPath);
            log.info("Attachment path = {}, csv files in path = {}", attachmentPath, attachments);
            if (CollectionUtils.isNotEmpty(attachments)) {
                Optional<String> attachment = attachments.stream().filter(StringUtils::isNotBlank).findFirst();
                if (attachment.isPresent()) {
                    return IOUtils.toByteArray(HdfsUtils.getInputStream(yarnConfiguration, attachment.get()));
                }
            }
            log.warn("Attachment file does not exist");
        } catch (Exception e) {
            log.error("Failed to get intent email attachment", e);
        }
        return null;
    }

    private String getSummaryText(int numBuyIntents, int numResearchIntents) {
        if (numBuyIntents >= numResearchIntents) {
            return "You have companies with high buying intent this week.";
        } else {
            return "You have companies with high researching intent this week.";
        }
    }

    private void addToCountMap(Map<String, IntentAlertEmailInfo.TopItem> map, String name) {
        if (StringUtils.isBlank(name)) {
            return;
        }
        IntentAlertEmailInfo.TopItem topItem = map.get(name);
        if (topItem == null) {
            topItem = new IntentAlertEmailInfo.TopItem();
            topItem.setName(name);
            map.put(name, topItem);
        }
        topItem.increaseNumIntents();
    }

    private void addToModelMap(Map<String, List<IntentAlertEmailInfo.Intent>> map,
            IntentAlertEmailInfo.Intent subscriptionItem) {
        String modelName = subscriptionItem.getModel();
        if (StringUtils.isNotBlank(modelName)) {
            map.putIfAbsent(modelName, new ArrayList<>());
            map.get(modelName).add(subscriptionItem);
        }
    }

    private Object getTopListFromMap(Map<String, IntentAlertEmailInfo.TopItem> map) {
        List<IntentAlertEmailInfo.TopItem> topList = map.values().stream().collect(Collectors.toList());
        topList.sort(Comparator.comparing(IntentAlertEmailInfo.TopItem::getNumIntents).reversed());
        topList = topList.size() > IntentAlertEmailInfo.TOPLIMIT ? topList.subList(0, IntentAlertEmailInfo.TOPLIMIT)
                : topList;
        updatePercentage(topList);
        return toJsonObject(topList);
    }

    private List<List<HashMap<String, Object>>> getModelPairFromMap(
            Map<String, List<IntentAlertEmailInfo.Intent>> map) {
        List<List<IntentAlertEmailInfo.Intent>> sortedList = map.values().stream().collect(Collectors.toList());
        sortedList.sort(Comparator.comparing(list -> list.size()));
        List<List<HashMap<String, Object>>> pairList = new ArrayList<>();
        int index = 0;
        List<HashMap<String, Object>> pair = new ArrayList<>();
        for (List<IntentAlertEmailInfo.Intent> list : sortedList) {
            if ((index++ & 1) == 0) {
                pair = new ArrayList<>();
                pairList.add(pair);
            }
            HashMap<String, Object> intentMap = new HashMap<>();
            list.sort(Comparator.comparing(IntentAlertEmailInfo.Intent::getStageCompareInt));
            intentMap.put("name", list.get(0).getModel());
            intentMap.put("intents", toJsonObject(list));
            pair.add(intentMap);
        }
        return pairList;
    }

    private Object toJsonObject(List<?> list) {
        return JsonUtils.deserialize(JsonUtils.serialize(list), List.class);
    }

    private void updatePercentage(List<IntentAlertEmailInfo.TopItem> list) {
        if (CollectionUtils.isNotEmpty(list)) {
            double max = Collections.max(list, Comparator.comparing(IntentAlertEmailInfo.TopItem::getNumIntents))
                    .getNumIntents();
            for (IntentAlertEmailInfo.TopItem item : list) {
                item.setBuyPercentage(item.getNumBuy() / max);
                item.setResearchPercentage(item.getNumResearch() / max);
            }
        }
    }

    private boolean stageEqual(IntentAlertEmailInfo.Intent intent, IntentAlertEmailInfo.StageType type) {
        return type.toString().equalsIgnoreCase(intent.getStage());
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
