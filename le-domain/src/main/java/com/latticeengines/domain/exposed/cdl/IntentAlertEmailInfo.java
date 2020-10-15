package com.latticeengines.domain.exposed.cdl;

import java.io.Serializable;

import org.apache.avro.generic.GenericRecord;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IntentAlertEmailInfo {

    protected IntentAlertEmailInfo() {
        throw new UnsupportedOperationException();
    }

    public static final int TOPLIMIT = 5;

    public enum StageType {

        BUY("buying"), RESEARCH("researching");

        private String type;

        StageType(String type) {
            this.type = type;
        }

        @Override
        public String toString() {
            return this.type;
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

        public void setStage(String stage) {
            this.stage = stage;
        }

        public Intent(GenericRecord record) {
            this.industry = getRecordByFieldName(record, "LDC_PrimaryIndustry");
            this.location = getRecordByFieldName(record, "STATE_PROVINCE_ABBR");
            this.model = getRecordByFieldName(record, "ModelName");
            this.companyName = getRecordByFieldName(record, "LDC_Name");
            this.stage = getRecordByFieldName(record, "Stage");
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
