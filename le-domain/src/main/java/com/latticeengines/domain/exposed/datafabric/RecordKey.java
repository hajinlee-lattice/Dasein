package com.latticeengines.domain.exposed.datafabric;

public class RecordKey {

    private String id;

    private String customerSpace;

    private String producer;

    private String version;

    private String recordType;

    private long timeStamp;

    public RecordKey(Builder builder) {
        this.id = builder.id;
        this.customerSpace = builder.customerSpace;
        this.producer = builder.producer;
        this.version = builder.version;
        this.recordType = builder.recordType;
        this.timeStamp = builder.timeStamp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(String customerSpace) {
        this.customerSpace = customerSpace;
    }

    public String getProducer() {
        return producer;
    }

    public void setProducer(String producer) {
        this.producer = producer;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getRecordType() {
        return recordType;
    }

    public void setRecordType(String recordType) {
        this.recordType = recordType;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public static class Builder {
        private String id;

        private String customerSpace;

        private String producer;

        private String version;

        private String recordType;

        private long timeStamp;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder customerSpace(String customerSpace) {
            this.customerSpace = customerSpace;
            return this;
        }

        public Builder producer(String producer) {
            this.producer = producer;
            return this;
        }

        public Builder version(String version) {
            this.version = version;
            return this;
        }

        public Builder recordType(String recordType) {
            this.recordType = recordType;
            return this;
        }

        public Builder timeStamp(long timeStamp) {
            this.timeStamp = timeStamp;
            return this;
        }

        public RecordKey build() {
            return new RecordKey(this);
        }
    }
}
