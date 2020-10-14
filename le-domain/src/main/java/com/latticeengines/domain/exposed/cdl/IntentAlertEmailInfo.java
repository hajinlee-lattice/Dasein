package com.latticeengines.domain.exposed.cdl;

import java.io.Serializable;

import org.apache.avro.generic.GenericRecord;

public class IntentAlertEmailInfo {

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

        private String industry;

        private String location;

        private String model;

        private String company_name;

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

        public String getCompany_name() {
            return company_name;
        }

        public void setCompany_name(String company_name) {
            this.company_name = company_name;
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
            this.company_name = getRecordByFieldName(record, "LDC_Name");
            this.stage = getRecordByFieldName(record, "Stage");
        }

        private String getRecordByFieldName(GenericRecord record, String name) {
            return record.get(name) == null ? "default" : record.get(name).toString();
        }
    }

    public static class TopItem {
        private String name;
        private int num_intents;
        private int num_buy;
        private int num_research;
        private double buy_percentage;
        private double research_percentage;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getNum_buy() {
            return num_buy;
        }

        public void setNum_buy(int num_buy) {
            this.num_buy = num_buy;
        }

        public int getNum_research() {
            return num_research;
        }

        public void setNum_research(int num_research) {
            this.num_research = num_research;
        }

        public double getBuy_percentage() {
            return buy_percentage;
        }

        public void setBuy_percentage(double buy_percentage) {
            this.buy_percentage = buy_percentage;
        }

        public double getResearch_percentage() {
            return research_percentage;
        }

        public void setResearch_percentage(double research_percentage) {
            this.research_percentage = research_percentage;
        }

        public int getNum_intents() {
            return num_intents;
        }

        public void setNum_intents(int num_intents) {
            this.num_intents = num_intents;
        }

        public int increaseNum_intents() {
            return ++num_intents;
        }

        public int increaseNum_buy() {
            return ++num_buy;
        }

        public int increaseNum_research() {
            return ++num_research;
        }
    }
}
