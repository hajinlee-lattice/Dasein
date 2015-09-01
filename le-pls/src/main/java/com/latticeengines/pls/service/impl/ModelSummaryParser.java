package com.latticeengines.pls.service.impl;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.KeyValue;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.util.ModelIdUtils;

@Component("modelSummaryParser")
public class ModelSummaryParser {

    public ModelSummary parse(String hdfsPath, String fileContents) {

        if (fileContents == null) { return null; }

        ModelSummary summary = new ModelSummary();
        try {
            KeyValue keyValue = new KeyValue();
            keyValue.setData(CompressionUtils.compressByteArray(fileContents.getBytes()));
            summary.setDetails(keyValue);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18020, new String[] { hdfsPath });
        }

        ObjectMapper mapper = new ObjectMapper();
        JsonNode json;
        try {
            json = mapper.readValue(fileContents, JsonNode.class);
        } catch (IOException e) {
            // ignore
            return null;
        }

        JsonNode details = json.get("ModelDetails");

        String name = getOrDefault(details.get("Name"), String.class, "PLS");
        Long constructionTime;
        try {
            long currentMillis = details.get("ConstructionTime").asLong() * 1000;
            getDate(currentMillis, "MM/dd/yyyy hh:mm:ss z");
            constructionTime = currentMillis;
        } catch (Exception e) {
            constructionTime = System.currentTimeMillis();
        }
        String lookupId = getOrDefault(details.get("LookupID"), String.class, "");
        summary.setName(String.format("%s-%s", name.replace(' ', '_'), getDate(constructionTime, "MM/dd/yyyy hh:mm:ss z")));
        summary.setLookupId(lookupId);
        summary.setRocScore(getOrDefault(details.get("RocScore"), Double.class, 0.0));
        summary.setTrainingRowCount(getOrDefault(details.get("TrainingLeads"), Long.class, 0L));
        summary.setTestRowCount(getOrDefault(details.get("TestingLeads"), Long.class, 0L));
        summary.setTotalRowCount(getOrDefault(details.get("TotalLeads"), Long.class, 0L));
        summary.setTrainingConversionCount(getOrDefault(details.get("TrainingConversions"), Long.class, 0L));
        summary.setTestConversionCount(getOrDefault(details.get("TestingConversions"), Long.class, 0L));
        summary.setTotalConversionCount(getOrDefault(details.get("TotalConversions"), Long.class, 0L));
        summary.setConstructionTime(constructionTime);
        summary.setIncomplete(isIncomplete(json));

        if (details.has("ModelID")) {
            summary.setId(details.get("ModelID").asText());
        } else if (details.has("LookupID")) {
            String uuid = details.get("LookupID").asText().split("\\|")[2];
            summary.setId(String.format("ms__%s-%s", uuid, name));
        } else {
            String uuid = UUID.randomUUID().toString();
            summary.setId(String.format("ms__%s-%s", uuid, name));
        }

        // the Id will be used to find hdfs path, make sure they are in sync.
        try {
            String uuidInPath = ModelIdUtils.extractUuid(hdfsPath);
            String uuidInId = ModelIdUtils.extractUuid(summary.getId());
            if (! uuidInPath.equals(uuidInId)) {
                summary.setId("ms__" + uuidInPath + "-PLSModel");
            }
        } catch (Exception e) {
            // ignore
        }

        try {
            if (json.has("Tenant")) {
                summary.setTenant(mapper.treeToValue(json.get("Tenant"), Tenant.class));
            } else if (details.has("Tenant")) {
                summary.setTenant(mapper.treeToValue(details.get("Tenant"), Tenant.class));
            } else {
                Tenant tenant = new Tenant();
                tenant.setPid(-1L);
                tenant.setRegisteredTime(System.currentTimeMillis());
                tenant.setId("FAKE_TENANT");
                tenant.setName("Fake Tenant");
                summary.setTenant(tenant);
            }
        } catch (JsonProcessingException e) {
            // ignore
        }

        return summary;
    }

    private String getDate(long milliSeconds, String dateFormat) {
        SimpleDateFormat formatter = new SimpleDateFormat(dateFormat);
        formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(milliSeconds);
        return formatter.format(calendar.getTime());
    }

    private <T> T getOrDefault(JsonNode node, Class<T> targetClass, T defaultValue) {
        if (node == null) { return defaultValue; }
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.treeToValue(node, targetClass);
        } catch (JsonProcessingException e) {
            return defaultValue;
        }
    }

    public String parseOriginalName(String nameDatetime) {
        String dateTimePattern = "(0[1-9]|1[012])/(0[1-9]|[12][0-9]|3[01])/(19|20)\\d\\d";
        Pattern pattern = Pattern.compile(dateTimePattern);
        Matcher matcher = pattern.matcher(nameDatetime);
        matcher.find();
        return nameDatetime.substring(0, matcher.start() - 1);
    }

    public boolean isIncomplete(JsonNode summaryJson) {
        return !(summaryJson.has("Segmentations") &&
                summaryJson.has("Predictors") &&
                summaryJson.has("ModelDetails") &&
                summaryJson.has("TopSample") &&
                summaryJson.has("BottomSample") &&
                summaryJson.has("EventTableProvenance"));
    }
}
