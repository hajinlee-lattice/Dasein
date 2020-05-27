package com.latticeengines.pls.controller.datacollection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.app.exposed.service.CommonTenantConfigService;
import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.util.RestrictionUtils;
import com.latticeengines.pls.service.impl.GraphDependencyToUIActionUtil;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

public abstract class BaseFrontEndEntityResource {

    private static final Logger log = LoggerFactory.getLogger(BaseFrontEndEntityResource.class);

    private static final String NEW_SEGMENT_PLACEHOLDER = "Create";

    private final EntityProxy entityProxy;

    private final SegmentProxy segmentProxy;

    private final DataCollectionProxy dataCollectionProxy;

    private final GraphDependencyToUIActionUtil graphDependencyToUIActionUtil;

    @Inject
    private CommonTenantConfigService tenantConfigService;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private DataLakeService dataLakeService;

    BaseFrontEndEntityResource(EntityProxy entityProxy, SegmentProxy segmentProxy,
            DataCollectionProxy dataCollectionProxy, GraphDependencyToUIActionUtil graphDependencyToUIActionUtil) {
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
                cleanupAndValidateBucketsInQuery(frontEndQuery);
            } catch (LedpException e) {
                if (LedpCode.LEDP_40057.equals(e.getCode())) {
                    throw graphDependencyToUIActionUtil.handleInvalidBucketsError(e, //
                            "Failed to get " + mainEntity + " count");
                }
            }
        }
        appendSegmentRestrictionAndFreeTextSearch(frontEndQuery, mainEntity);
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
        return getData(frontEndQuery, getMainEntity());
    }

    public DataPage getData(FrontEndQuery frontEndQuery, BusinessEntity mainEntity) {
        String tenantId = MultiTenantContext.getCustomerSpace().getTenantId();
        String servingTableName = dataCollectionProxy.getTableName(tenantId, mainEntity.getServingStore());
        if (StringUtils.isBlank(servingTableName)) {
            log.warn(String.format("%s's serving store %s does not exist, returning empty data page.",
                    mainEntity.name(), mainEntity.getServingStore().name()));
            return new DataPage();
        }
        if (frontEndQuery == null) {
            frontEndQuery = new FrontEndQuery();
        }
        if (BusinessEntity.Product.equals(mainEntity)) {
            frontEndQuery.setAccountRestriction(null);
            frontEndQuery.setContactRestriction(null);
        } else {
            try {
                cleanupAndValidateBucketsInQuery(frontEndQuery);
            } catch (LedpException e) {
                if (LedpCode.LEDP_40057.equals(e.getCode())) {
                    throw graphDependencyToUIActionUtil.handleInvalidBucketsError(e, //
                            "Failed to get " + mainEntity + " data");
                }
            }
        }
        appendSegmentRestrictionAndFreeTextSearch(frontEndQuery, mainEntity);
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
        frontEndQuery.setMainEntity(mainEntity);
        if (CollectionUtils.isEmpty(frontEndQuery.getLookups())) {
            frontEndQuery.setLookups(getDataLookups());
        }
        ArrayList<Lookup> lookups = new ArrayList<>(frontEndQuery.getLookups());
        boolean companyNameExpanded = expandCompanyNameLookup(lookups);
        Map<String, String> replacedAttrs = replaceDataLookups(lookups);
        frontEndQuery.setLookups(lookups);

        DataPage data = entityProxy.getData(tenantId, frontEndQuery);
        if (data != null && CollectionUtils.isNotEmpty(data.getData())) {
            data.getData().forEach(map -> postProcessRecord(map, companyNameExpanded, replacedAttrs));
        }
        return data;
    }

    private void cleanupAndValidateBucketsInQuery(FrontEndQuery frontEndQuery) throws LedpException {
        Restriction accountRestriction = (frontEndQuery.getAccountRestriction() != null) ? //
                frontEndQuery.getAccountRestriction().getRestriction() : null;
        Restriction contactRestriction = (frontEndQuery.getContactRestriction() != null) ? //
                frontEndQuery.getContactRestriction().getRestriction() : null;
        RestrictionUtils.cleanupBucketsInRestriction(accountRestriction);
        RestrictionUtils.cleanupBucketsInRestriction(contactRestriction);
        List<BucketRestriction> invalidBkts = new ArrayList<>();
        try {
            invalidBkts.addAll(RestrictionUtils.validateBktsInRestriction(accountRestriction));
            invalidBkts.addAll(RestrictionUtils.validateBktsInRestriction(contactRestriction));
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_40057, e, new String[] { e.getMessage() });
        }
        if (CollectionUtils.isNotEmpty(invalidBkts)) {
            String message = invalidBkts.stream() //
                    .map(BucketRestriction::getAttr) //
                    .map(AttributeLookup::toString) //
                    .collect(Collectors.joining(","));
            throw new LedpException(LedpCode.LEDP_40057, new String[] { message });
        }
    }

    private void appendSegmentRestrictionAndFreeTextSearch(FrontEndQuery frontEndQuery, BusinessEntity mainEntity) {
        // restrictions in the query
        Restriction frontEndAccountRestriction = frontEndQuery.getAccountRestriction() != null
                ? frontEndQuery.getAccountRestriction().getRestriction()
                : null;
        Restriction frontEndContactRestriction = frontEndQuery.getContactRestriction() != null
                ? frontEndQuery.getContactRestriction().getRestriction()
                : null;

        // restrictions in the segment
        Restriction segmentAccountRestriction = null;
        Restriction segmentContactRestriction = null;
        if (StringUtils.isNotBlank(frontEndQuery.getPreexistingSegmentName())
                && !NEW_SEGMENT_PLACEHOLDER.equals(frontEndQuery.getPreexistingSegmentName())) {
            Pair<Restriction, Restriction> segmentRestrictions = getSegmentRestrictions(
                    frontEndQuery.getPreexistingSegmentName());
            segmentAccountRestriction = segmentRestrictions.getLeft();
            segmentContactRestriction = segmentRestrictions.getRight();
        }

        // free text search
        Restriction freeTextAccountRestriction = //
                translateFreeTextSearchForAccount(frontEndQuery.getFreeFormTextSearch(), mainEntity);
        Restriction freeTextContactRestriction = //
                translateFreeTextSearchForContact(frontEndQuery.getFreeFormTextSearch(), mainEntity);
        frontEndQuery.setFreeFormTextSearch(null);

        // merge together
        Restriction finalAccountRestriction = tripleMergeRestrictions( //
                segmentAccountRestriction, frontEndAccountRestriction, freeTextAccountRestriction //
        );
        Restriction finalContactRestriction = tripleMergeRestrictions( //
                segmentContactRestriction, frontEndContactRestriction, freeTextContactRestriction //
        );

        frontEndQuery.setAccountRestriction(new FrontEndRestriction(finalAccountRestriction));
        frontEndQuery.setContactRestriction(new FrontEndRestriction(finalContactRestriction));
    }

    private Restriction tripleMergeRestrictions(Restriction r1, Restriction r2, Restriction r3) {
        List<Restriction> nonEmptyRs = Stream.of(r1, r2, r3).filter(Objects::nonNull).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(nonEmptyRs)) {
            return Restriction.builder().and(Collections.emptyList()).build();
        } else if (CollectionUtils.size(nonEmptyRs) == 1) {
            return nonEmptyRs.get(0);
        } else {
            return Restriction.builder().and(nonEmptyRs).build();
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

    abstract BusinessEntity getMainEntity();

    abstract List<Lookup> getDataLookups();

    private Map<String, String> replaceDataLookups(List<Lookup> lookups) {
        return new HashMap<>();
    }

    private boolean expandCompanyNameLookup(ArrayList<Lookup> lookups) {
        boolean hasCompanyName = false;
        boolean hasLdcName = false;
        for (Lookup lookup : lookups) {
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

    private void postProcessRecord(Map<String, Object> result, boolean companyNameExpanded, //
            Map<String, String> replacedAttrs) {
        if (companyNameExpanded) {
            mergeCompanyName(result);
        }
        if (MapUtils.isNotEmpty(replacedAttrs)) {
            resumeReplacedAttrs(result, replacedAttrs);
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

    private void resumeReplacedAttrs(Map<String, Object> result, Map<String, String> replacedAttrs) {
        for (String replacedAttr : replacedAttrs.keySet()) {
            if (result.containsKey(replacedAttr)) {
                String originalAttr = replacedAttrs.get(replacedAttr);
                Object value = result.get(replacedAttr);
                result.remove(replacedAttr);
                result.put(originalAttr, value);
            }
        }
    }

    private Restriction translateFreeTextSearchForAccount(String freeTextSearch, BusinessEntity queryEntity) {
        return translateFreeTextSearch(BusinessEntity.Account, freeTextSearch, queryEntity);
    }

    private Restriction translateFreeTextSearchForContact(String freeTextSearch, BusinessEntity queryEntity) {
        return translateFreeTextSearch(BusinessEntity.Contact, freeTextSearch, queryEntity);
    }

    private Restriction translateFreeTextSearch(BusinessEntity searchEntity, String freeTextSearch, //
            BusinessEntity queryEntity) {
        if (StringUtils.isBlank(freeTextSearch)) {
            return null;
        }

        ConcurrentMap<BusinessEntity, Set<String>> servingStoreAttrs = new ConcurrentHashMap<>();
        String tenantId = MultiTenantContext.getShortTenantId();

        // PLS-15419 Check that columns exist for the searchAttr
        List<String> columnAttrNames = dataLakeService.getCachedServingMetadataForEntity( //
                MultiTenantContext.getCustomerSpace().toString(), searchEntity).stream() //
                .filter(columnMetadata -> columnMetadata.isEnabledFor(ColumnSelection.Predefined.Segment)) //
                .map(ColumnMetadata::getAttrName) //
                .collect(Collectors.toList());

        List<AttributeLookup> searchAttrs = getFreeTextSearchAttrs(queryEntity).stream() //
                .filter(attributeLookup -> searchEntity.equals(attributeLookup.getEntity())) //
                .filter(attributeLookup -> {
                    String attrName = attributeLookup.getAttribute();
                    if (InterfaceName.AccountId.name().equals(attrName) && isEntityMatchGA()) {
                        // for entity match GA, always add AccountId to free-text search
                        return true;
                    } else {
                        return columnAttrNames.contains(attributeLookup.getAttribute());
                    }
                }) //
                .filter(attributeLookup -> {
                    String attrName = attributeLookup.getAttribute();
                    BusinessEntity entity = attributeLookup.getEntity();
                    if (!servingStoreAttrs.containsKey(entity)) {
                        servingStoreAttrs.put(entity,
                                servingStoreProxy.getServingStoreColumnsFromCache(tenantId, entity));
                    }
                    return servingStoreAttrs.get(entity).contains(attrName);
                }) //
                .collect(Collectors.toList());

        List<Restriction> searchRestrictions = searchAttrs.stream().map(attributeLookup -> {
            Bucket bucket = new Bucket();
            bucket.setComparisonType(ComparisonType.CONTAINS);
            bucket.setValues(Collections.singletonList(freeTextSearch));
            return new BucketRestriction(attributeLookup, bucket);
        }).collect(Collectors.toList());

        if (CollectionUtils.isEmpty(searchRestrictions)) {
            return null;
        } else if (CollectionUtils.size(searchRestrictions) == 1) {
            return searchRestrictions.get(0);
        } else {
            return Restriction.builder().or(searchRestrictions).build();
        }
    }

    private boolean isEntityMatchGA() {
        return tenantConfigService.onlyEntityMatchGAEnabled();
    }

    private List<AttributeLookup> getFreeTextSearchAttrs(BusinessEntity queryEntity) {
        switch (queryEntity) {
        case Account:
            return Arrays.asList( //
                    new AttributeLookup(BusinessEntity.Account, InterfaceName.CompanyName.name()), //
                    new AttributeLookup(BusinessEntity.Account, InterfaceName.Website.name()), //
                    new AttributeLookup(BusinessEntity.Account, InterfaceName.City.name()), //
                    new AttributeLookup(BusinessEntity.Account, InterfaceName.State.name()), //
                    new AttributeLookup(BusinessEntity.Account, InterfaceName.Country.name()), //
                    new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name()) //
            );
        case Contact:
            return Arrays.asList( //
                    new AttributeLookup(BusinessEntity.Contact, InterfaceName.ContactName.name()), //
                    new AttributeLookup(BusinessEntity.Contact, InterfaceName.CompanyName.name()), //
                    new AttributeLookup(BusinessEntity.Contact, InterfaceName.Email.name()) //
            );
        default:
            return Collections.emptyList();
        }
    }

}
