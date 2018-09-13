package com.latticeengines.pls.controller.datacollection;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

public abstract class BaseFrontEndEntityResource {

    private static final Logger log = LoggerFactory.getLogger(BaseFrontEndEntityResource.class);

    private final EntityProxy entityProxy;

    private final SegmentProxy segmentProxy;

    private final DataCollectionProxy dataCollectionProxy;

    BaseFrontEndEntityResource(EntityProxy entityProxy, SegmentProxy segmentProxy,
                               DataCollectionProxy dataCollectionProxy) {
        this.entityProxy = entityProxy;
        this.segmentProxy = segmentProxy;
        this.dataCollectionProxy = dataCollectionProxy;
    }

    public Long getCount(FrontEndQuery frontEndQuery) {
        String tenantId = MultiTenantContext.getCustomerSpace().getTenantId();
        return getCount(tenantId, frontEndQuery, getMainEntity());
    }

    protected Long getCount(String tenantId, FrontEndQuery frontEndQuery, BusinessEntity mainEntity) {
        String servingTableName = dataCollectionProxy.getTableName(tenantId, mainEntity.getServingStore());
        if (StringUtils.isBlank(servingTableName)) {
            log.warn(String.format("%s's serving store %s does not exist, returning 0 count.", mainEntity.name(),
                    mainEntity.getServingStore().name()));
            return 0L;
        }
        if (frontEndQuery == null) {
            frontEndQuery = new FrontEndQuery();
        }
        if (BusinessEntity.Product.equals(mainEntity)) {
            frontEndQuery.setAccountRestriction(null);
            frontEndQuery.setContactRestriction(null);
        }
        appendSegmentRestriction(frontEndQuery);
        frontEndQuery.setMainEntity(mainEntity);
        if (BusinessEntity.Contact.equals(mainEntity)) {
            Restriction accountRestriction;
            if (frontEndQuery.getAccountRestriction() != null
                    && frontEndQuery.getAccountRestriction().getRestriction() != null) {
                accountRestriction = appendAccountNotNull(frontEndQuery.getAccountRestriction().getRestriction());
            } else {
                accountRestriction = accountNotNullBucket();
            }
            frontEndQuery.setAccountRestriction(new FrontEndRestriction(accountRestriction));
        }
        return entityProxy.getCount(tenantId, frontEndQuery);
    }

    public DataPage getData(FrontEndQuery frontEndQuery) {
        String tenantId = MultiTenantContext.getCustomerSpace().getTenantId();
        String servingTableName = dataCollectionProxy.getTableName(tenantId, getMainEntity().getServingStore());
        if (StringUtils.isBlank(servingTableName)) {
            log.warn(String.format("%s's serving store %s does not exist, returning empty data page.",
                    getMainEntity().name(), getMainEntity().getServingStore().name()));
            return new DataPage();
        }
        if (frontEndQuery == null) {
            frontEndQuery = new FrontEndQuery();
        }
        if (BusinessEntity.Product.equals(getMainEntity())) {
            frontEndQuery.setAccountRestriction(null);
            frontEndQuery.setContactRestriction(null);
        }
        appendSegmentRestriction(frontEndQuery);
        frontEndQuery.setMainEntity(getMainEntity());
        if (CollectionUtils.isEmpty(frontEndQuery.getLookups())) {
            frontEndQuery.setLookups(getDataLookups());
        }
        DataPage data = entityProxy.getData(tenantId, frontEndQuery);
        if (data != null && CollectionUtils.isNotEmpty(data.getData())) {
            data.getData().forEach(this::postProcessRecord);
        }
        return data;
    }

    private void appendSegmentRestriction(FrontEndQuery frontEndQuery) {
        if (StringUtils.isNotBlank(frontEndQuery.getPreexistingSegmentName())
                && !"Create".equals(frontEndQuery.getPreexistingSegmentName())) {
            // Segment Restrictions
            Pair<Restriction, Restriction> segmentRestrictions = getSegmentRestrictions(
                    frontEndQuery.getPreexistingSegmentName());
            Restriction segmentAccountRestriction = segmentRestrictions.getLeft();
            Restriction segmentContactRestriction = segmentRestrictions.getRight();

            // Account
            if (segmentAccountRestriction != null) {
                Restriction frontEndAccountRestriction = frontEndQuery.getAccountRestriction() != null
                        ? frontEndQuery.getAccountRestriction().getRestriction() : null;
                if (frontEndAccountRestriction != null) {
                    Restriction totalRestriction = Restriction.builder()
                            .and(frontEndAccountRestriction, segmentAccountRestriction).build();
                    frontEndQuery.getAccountRestriction().setRestriction(totalRestriction);
                } else {
                    FrontEndRestriction feRestriction = frontEndQuery.getAccountRestriction();
                    if (feRestriction == null) {
                        feRestriction = new FrontEndRestriction();
                    }
                    feRestriction.setRestriction(segmentAccountRestriction);
                    frontEndQuery.setAccountRestriction(feRestriction);
                }
            }

            // Contact
            if (segmentContactRestriction != null) {
                Restriction frontEndContactRestriction = frontEndQuery.getContactRestriction() != null
                        ? frontEndQuery.getContactRestriction().getRestriction() : null;
                if (frontEndContactRestriction != null) {
                    Restriction totalRestriction = Restriction.builder()
                            .and(frontEndContactRestriction, segmentContactRestriction).build();
                    frontEndQuery.getContactRestriction().setRestriction(totalRestriction);
                } else {
                    FrontEndRestriction feRestriction = frontEndQuery.getContactRestriction();
                    if (feRestriction == null) {
                        feRestriction = new FrontEndRestriction();
                    }
                    feRestriction.setRestriction(segmentContactRestriction);
                    frontEndQuery.setContactRestriction(feRestriction);
                }
            }
        }
    }

    private Restriction appendAccountNotNull(Restriction restriction) {
        if (restriction == null) {
            return accountNotNullBucket();
        } else {
            return Restriction.builder().and(restriction, accountNotNullBucket()).build();
        }
    }

    private BucketRestriction accountNotNullBucket() {
        Bucket bkt = Bucket.notNullBkt();
        return new BucketRestriction(BusinessEntity.Account, InterfaceName.AccountId.name(), bkt);
    }

    private Pair<Restriction, Restriction> getSegmentRestrictions(String segmentName) {
        MetadataSegment segment = segmentProxy
                .getMetadataSegmentByName(MultiTenantContext.getCustomerSpace().toString(), segmentName);
        if (segment != null) {
            return Pair.of(segment.getAccountRestriction(), segment.getContactRestriction());
        } else {
            return Pair.of(null, null);
        }
    }

    protected void postProcessRecord(Map<String, Object> result) {
    }

    void overwriteCompanyName(Map<String, Object> result) {
        if (result.containsKey(InterfaceName.CompanyName.toString())
                && result.containsKey(InterfaceName.LDC_Name.toString())) {
            String companyName = (String) result.get(InterfaceName.CompanyName.toString());
            String ldcName = (String) result.get(InterfaceName.LDC_Name.toString());
            String consolidatedName = (ldcName != null) ? ldcName : companyName;
            if (consolidatedName != null && !consolidatedName.equals(companyName)) {
                result.put(InterfaceName.CompanyName.toString(), consolidatedName);
                //TODO: YSong-M23 temp log, to be removed
                log.info("Overwriting company name to " + consolidatedName);
            }
        }
    }

    abstract BusinessEntity getMainEntity();

    abstract List<Lookup> getDataLookups();

}
