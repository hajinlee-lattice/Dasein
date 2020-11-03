package com.latticeengines.domain.exposed.cdl;

import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.DnbIntent.STAGE_BUYING;
import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.DnbIntent.STAGE_RESEARCHING;

import java.io.Serializable;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IntentAlertEmailInfo {

    protected IntentAlertEmailInfo() {
        throw new UnsupportedOperationException();
    }

    public static final int TOPLIMIT = 5;

    public enum StageType {

        BUY(STAGE_BUYING, "Buying"), RESEARCH(STAGE_RESEARCHING, "Research");

        private String name;
        private String displayName;

        StageType(String name, String displayName) {
            this.name = name;
            this.displayName = displayName;
        }

        public static String parseStageName(String stageName) {
            for (StageType type : values()) {
                if (type.name.equalsIgnoreCase(stageName)) {
                    return type.toString();
                }
            }
            return stageName;
        }

        @Override
        public String toString() {
            return this.displayName;
        }
    }

    public static class Intent implements Serializable {

        @JsonProperty("industry")
        private String industry;

        @JsonProperty("location")
        private String location;

        @JsonProperty("model")
        private String model;

        @JsonProperty("company_name")
        private String companyName;

        @JsonProperty("stage")
        private String stage;

        public String getIndustry() {
            return industry;
        }

        public void setIndustry(String industry) {
            this.industry = industry;
        }

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }

        public String getModel() {
            return model;
        }

        public void setModel(String model) {
            this.model = model;
        }

        public String getCompanyName() {
            return companyName;
        }

        public void setCompanyName(String companyName) {
            this.companyName = companyName;
        }

        public String getStage() {
            return stage;
        }

        public int getStageCompareInt() {
            if (stage.equalsIgnoreCase(StageType.BUY.toString())) {
                return 0;
            } else if (stage.equalsIgnoreCase(StageType.RESEARCH.toString())) {
                return 1;
            } else if (StringUtils.isBlank(stage)) {
                return 100;
            } else {
                return 2;
            }
        }

        public void setStage(String stage) {
            this.stage = stage;
        }

        public Intent(GenericRecord record) {
            this.industry = getRecordByFieldName(record, "Industry");
            this.location = getRecordByFieldName(record, "Location");
            this.model = getRecordByFieldName(record, "ModelName");
            this.companyName = getRecordByFieldName(record, "CompanyName");
            this.stage = StageType.parseStageName(getRecordByFieldName(record, "Stage"));
        }

        private String getRecordByFieldName(GenericRecord record, String name) {
            return record.get(name) == null ? "" : record.get(name).toString();
        }
    }

    public static class TopItem {

        @JsonProperty("name")
        private String name;

        @JsonProperty("num_intents")
        private int numIntents;

        @JsonProperty("num_buy")
        private int numBuy;

        @JsonProperty("num_research")
        private int numResearch;

        @JsonProperty("buy_percentage")
        private double buyPercentage;

        @JsonProperty("research_percentage")
        private double researchPercentage;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getNumBuy() {
            return numBuy;
        }

        public void setNumBuy(int numBuy) {
            this.numBuy = numBuy;
        }

        public int getNumResearch() {
            return numResearch;
        }

        public void setNumResearch(int numResearch) {
            this.numResearch = numResearch;
        }

        public double getBuyPercentage() {
            return buyPercentage;
        }

        public void setBuyPercentage(double buyPercentage) {
            this.buyPercentage = buyPercentage;
        }

        public double getResearchPercentage() {
            return researchPercentage;
        }

        public void setResearchPercentage(double researchPercentage) {
            this.researchPercentage = researchPercentage;
        }

        public int getNumIntents() {
            return numIntents;
        }

        public void setNumIntents(int numIntents) {
            this.numIntents = numIntents;
        }

        public int increaseNumIntents() {
            return ++numIntents;
        }

        public int increaseNumBuy() {
            return ++numBuy;
        }

        public int increaseNumResearch() {
            return ++numResearch;
        }
    }
}
