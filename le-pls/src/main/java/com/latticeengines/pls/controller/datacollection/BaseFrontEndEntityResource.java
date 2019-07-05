package com.latticeengines.pls.controller.datacollection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.util.RestrictionUtils;
import com.latticeengines.pls.service.impl.GraphDependencyToUIActionUtil;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

public abstract class BaseFrontEndEntityResource {

    private static final Logger log = LoggerFactory.getLogger(BaseFrontEndEntityResource.class);

    private final EntityProxy entityProxy;

    private final SegmentProxy segmentProxy;

    private final DataCollectionProxy dataCollectionProxy;

    private final GraphDependencyToUIActionUtil graphDependencyToUIActionUtil;

    BaseFrontEndEntityResource(EntityProxy entityProxy, SegmentProxy segmentProxy,
                               DataCollectionProxy dataCollectionProxy,
                               GraphDependencyToUIActionUtil graphDependencyToUIActionUtil) {
        this.entityProxy = entityProxy;
        this.segmentProxy = segmentProxy;
        this.dataCollectionProxy = dataCollectionProxy;
        this.graphDependencyToUIActionUtil = graphDependencyToUIActionUtil;
    }

    public Long getCount(FrontEndQuery frontEndQuery) {
        String tenantId = MultiTenantContext.getCustomerSpace().getTenantId();
        return getCount(tenantId, frontEndQuery, getMainEntity());
    }

    Long getCount(String tenantId, FrontEndQuery frontEndQuery, BusinessEntity mainEntity) {
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
        } else {
            try {
                validateBucketsInQuery(frontEndQuery);
            } catch (LedpException e) {
                if (LedpCode.LEDP_40057.equals(e.getCode())) {
                    throw graphDependencyToUIActionUtil.handleInvalidBucketsError(e, //
                            "Failed to get " + getMainEntity() + " count");
                }
            }
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
        } else {
            try {
                validateBucketsInQuery(frontEndQuery);
            } catch (LedpException e) {
                if (LedpCode.LEDP_40057.equals(e.getCode())) {
                    throw graphDependencyToUIActionUtil.handleInvalidBucketsError(e, //
                            "Failed to get " + getMainEntity() + " data");
                }
            }
        }
        appendSegmentRestriction(frontEndQuery);
        frontEndQuery.setMainEntity(getMainEntity());
        if (CollectionUtils.isEmpty(frontEndQuery.getLookups())) {
            frontEndQuery.setLookups(getDataLookups());
        }
        ArrayList<Lookup> lookups = new ArrayList<>(frontEndQuery.getLookups());
        boolean companyNameExpanded = expandCompanyNameLookup(lookups);
        frontEndQuery.setLookups(lookups);
        DataPage data = entityProxy.getData(tenantId, frontEndQuery);
        if (data != null && CollectionUtils.isNotEmpty(data.getData())) {
            data.getData().forEach(map -> postProcessRecord(map, companyNameExpanded));
        }
        return data;
    }

    private void validateBucketsInQuery(FrontEndQuery frontEndQuery) throws LedpException {
        Restriction accountRestriction = (frontEndQuery.getAccountRestriction() != null) ? //
                frontEndQuery.getAccountRestriction().getRestriction() : null;
        Restriction contactRestriction = (frontEndQuery.getContactRestriction() != null) ? //
                frontEndQuery.getContactRestriction().getRestriction() : null;
        List<BucketRestriction> invalidBkts = new ArrayList<>();
        try {
            invalidBkts.addAll(RestrictionUtils.validateBktsInRestriction(accountRestriction));
            invalidBkts.addAll(RestrictionUtils.validateBktsInRestriction(contactRestriction));
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_40057, e, new String[]{ e.getMessage() });
        }
        if (CollectionUtils.isNotEmpty(invalidBkts)) {
            String message = invalidBkts.stream() //
                    .map(BucketRestriction::getAttr) //
                    .map(AttributeLookup::toString) //
                    .collect(Collectors.joining(","));
            throw new LedpException(LedpCode.LEDP_40057, new String[]{ message });
        }
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

    protected void postProcessRecord(Map<String, Object> result, boolean companyNameExpanded) {
        if (companyNameExpanded) {
            mergeCompanyName(result);
        }
    }

    private void mergeCompanyName(Map<String, Object> result) {
        if (result.containsKey(InterfaceName.CompanyName.toString())
                && result.containsKey(InterfaceName.LDC_Name.toString())) {
            String companyName = (String) result.get(InterfaceName.CompanyName.toString());
            String ldcName = (String) result.get(InterfaceName.LDC_Name.toString());
            String consolidatedName = (companyName != null) ? companyName : ldcName;
            result.put(InterfaceName.CompanyName.toString(), consolidatedName);
            result.remove(InterfaceName.LDC_Name.name());
        }
    }

    abstract BusinessEntity getMainEntity();

    abstract List<Lookup> getDataLookups();

    private boolean expandCompanyNameLookup(ArrayList<Lookup> lookups) {
        boolean hasCompanyName = false;
        boolean hasLdcName = false;
        for (Lookup lookup: lookups) {
            if (lookup instanceof AttributeLookup) {
                AttributeLookup attrLookup = (AttributeLookup) lookup;
                if (InterfaceName.CompanyName.name().equalsIgnoreCase(attrLookup.getAttribute())) {
                    hasCompanyName = true;
                }
                if (InterfaceName.LDC_Name.name().equalsIgnoreCase(((AttributeLookup) lookup).getAttribute())) {
                    hasLdcName = true;
                }
            }
        }
        if (hasCompanyName && !hasLdcName) {
            lookups.add(new AttributeLookup(BusinessEntity.Account, InterfaceName.LDC_Name.name()));
            return true;
        } else {
            return false;
        }
    }

}
