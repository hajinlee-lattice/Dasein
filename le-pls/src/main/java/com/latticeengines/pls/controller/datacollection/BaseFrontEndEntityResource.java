package com.latticeengines.pls.controller.datacollection;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
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
        return entityProxy.getData(tenantId, frontEndQuery);
    }

    private void appendSegmentRestriction(FrontEndQuery frontEndQuery) {
        if (StringUtils.isNotBlank(frontEndQuery.getPreexistingSegmentName())) {
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
                    frontEndQuery.getAccountRestriction().setRestriction(segmentAccountRestriction);
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
                    frontEndQuery.getContactRestriction().setRestriction(segmentContactRestriction);
                }
            }
        }
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

    abstract BusinessEntity getMainEntity();

}
