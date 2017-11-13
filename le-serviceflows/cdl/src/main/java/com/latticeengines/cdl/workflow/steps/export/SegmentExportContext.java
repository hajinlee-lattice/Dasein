package com.latticeengines.cdl.workflow.steps.export;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;

public class SegmentExportContext {

    private CustomerSpace customerSpace;

    private Tenant tenant;

    private MetadataSegmentExport metadataSegmentExport;

    private FrontEndQuery accountFrontEndQuery;

    private FrontEndQuery contactFrontEndQuery;

    private List<Object> modifiableAccountIdCollectionForContacts;

    private Counter counter;

    public SegmentExportContext(CustomerSpace customerSpace, Tenant tenant, MetadataSegmentExport metadataSegmentExport,
            FrontEndQuery accountFrontEndQuery, FrontEndQuery contactFrontEndQuery,
            List<Object> modifiableAccountIdCollectionForContacts, Counter counter) {
        super();
        this.customerSpace = customerSpace;
        this.tenant = tenant;
        this.metadataSegmentExport = metadataSegmentExport;
        this.accountFrontEndQuery = accountFrontEndQuery;
        this.contactFrontEndQuery = contactFrontEndQuery;
        this.modifiableAccountIdCollectionForContacts = modifiableAccountIdCollectionForContacts;
        this.counter = counter;
    }

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public MetadataSegmentExport getMetadataSegmentExport() {
        return metadataSegmentExport;
    }

    public FrontEndQuery getAccountFrontEndQuery() {
        return accountFrontEndQuery;
    }

    public FrontEndQuery getContactFrontEndQuery() {
        return contactFrontEndQuery;
    }

    public FrontEndQuery getClonedAccountFrontEndQuery() {
        return accountFrontEndQuery == null ? null : JsonUtils.clone(accountFrontEndQuery);
    }

    public FrontEndQuery getClonedContactFrontEndQuery() {
        return contactFrontEndQuery == null ? null : JsonUtils.clone(contactFrontEndQuery);
    }

    public List<Object> getModifiableAccountIdCollectionForContacts() {
        return modifiableAccountIdCollectionForContacts;
    }

    public Counter getCounter() {
        return counter;
    }

    public static class SegmentExportContextBuilder {
        private CustomerSpace customerSpace;

        private Tenant tenant;

        private MetadataSegmentExport metadataSegmentExport;

        private FrontEndQuery accountFrontEndQuery;

        private FrontEndQuery contactFrontEndQuery;

        private List<Object> modifiableAccountIdCollectionForContacts;

        private Counter counter;

        public SegmentExportContextBuilder() {
        }

        public SegmentExportContextBuilder customerSpace(CustomerSpace customerSpace) {
            this.customerSpace = customerSpace;
            return this;
        }

        public SegmentExportContextBuilder tenant(Tenant tenant) {
            this.tenant = tenant;
            return this;
        }

        public SegmentExportContextBuilder metadataSegmentExport(MetadataSegmentExport metadataSegmentExport) {
            this.metadataSegmentExport = metadataSegmentExport;
            return this;
        }

        public SegmentExportContextBuilder accountFrontEndQuery(FrontEndQuery accountFrontEndQuery) {
            this.accountFrontEndQuery = accountFrontEndQuery;
            return this;
        }

        public SegmentExportContextBuilder contactFrontEndQuery(FrontEndQuery contactFrontEndQuery) {
            this.contactFrontEndQuery = contactFrontEndQuery;
            return this;
        }

        public SegmentExportContextBuilder modifiableAccountIdCollectionForContacts(
                List<Object> modifiableAccountIdCollectionForContacts) {
            this.modifiableAccountIdCollectionForContacts = modifiableAccountIdCollectionForContacts;
            return this;
        }

        public SegmentExportContextBuilder counter(Counter counter) {
            this.counter = counter;
            return this;
        }

        public SegmentExportContext build() {
            return new SegmentExportContext(customerSpace, tenant, metadataSegmentExport, accountFrontEndQuery,
                    contactFrontEndQuery, modifiableAccountIdCollectionForContacts, counter);
        }
    }

    public static class Counter {
        private AtomicLong accountLaunched;
        private AtomicLong contactLaunched;
        private AtomicLong accountErrored;
        private AtomicLong accountSuppressed;

        public Counter() {
            accountLaunched = new AtomicLong();
            contactLaunched = new AtomicLong();
            accountErrored = new AtomicLong();
            accountSuppressed = new AtomicLong();
        }

        public AtomicLong getAccountLaunched() {
            return accountLaunched;
        }

        public AtomicLong getContactLaunched() {
            return contactLaunched;
        }

        public AtomicLong getAccountErrored() {
            return accountErrored;
        }

        public AtomicLong getAccountSuppressed() {
            return accountSuppressed;
        }
    }
}
