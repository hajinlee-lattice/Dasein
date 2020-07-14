package com.latticeengines.domain.exposed.cdl;

public class DynamoAccountLookupRecord {
    public String key;
    public String accountId;
    public boolean changed;
    public boolean deleted;

    public static final class Builder {

        private DynamoAccountLookupRecord lookupRecord;

        public Builder() {
            lookupRecord = new DynamoAccountLookupRecord();
        }

        public Builder key(String key) {
            lookupRecord.key = key;
            return this;
        }

        public Builder accountId(String accountId) {
            lookupRecord.accountId = accountId;
            return this;
        }

        public Builder changed(boolean changed) {
            lookupRecord.changed = changed;
            return this;
        }

        public Builder deleted(boolean deleted) {
            lookupRecord.deleted = deleted;
            return this;
        }

        public DynamoAccountLookupRecord build() {
            return lookupRecord;
        }
    }
}
