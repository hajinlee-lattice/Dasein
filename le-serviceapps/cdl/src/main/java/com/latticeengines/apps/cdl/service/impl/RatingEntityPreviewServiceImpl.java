package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.service.RatingEntityPreviewService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

@Component
public class RatingEntityPreviewServiceImpl implements RatingEntityPreviewService {

    private static final Logger log = LoggerFactory.getLogger(RatingEntityPreviewServiceImpl.class);

    @Inject
    private EntityProxy entityProxy;

    List<String> accountFields = Arrays.asList(InterfaceName.AccountId.name(), //
            InterfaceName.SalesforceAccountID.name(), //
            InterfaceName.CompanyName.name(), //
            InterfaceName.Domain.name(), //
            InterfaceName.Website.name(), //
            // InterfaceName.LastModifiedDate.name(), //
            InterfaceName.LDC_Name.name());

    List<String> contactFields = Arrays.asList(InterfaceName.AccountId.name(), //
            InterfaceName.ContactId.name(), //
            InterfaceName.CompanyName.name(), //
            InterfaceName.Email.name(), //
            InterfaceName.ContactName.name(), //
            InterfaceName.City.name(), //
            InterfaceName.State.name(), //
            InterfaceName.Country.name(), //
            InterfaceName.PostalCode.name(), //
            InterfaceName.PhoneNumber.name(), //
            InterfaceName.Title.name(), //
            InterfaceName.Address_Street_1.name());

    @Override
    public DataPage getEntityPreview(RatingEngine ratingEngine, long offset, long maximum, BusinessEntity entityType,
            Boolean restrictNotNullSalesforceId, List<String> selectedBuckets, String lookupIdColumn) {
        return getEntityPreview(ratingEngine, offset, maximum, entityType, null, false, null, null,
                restrictNotNullSalesforceId, null, selectedBuckets, lookupIdColumn);
    }

    @Override
    public DataPage getEntityPreview(RatingEngine ratingEngine, long offset, long maximum, BusinessEntity entityType,
            String sortBy, boolean descending, String bucketFieldName, List<String> lookupFieldNames,
            Boolean restrictNotNullSalesforceId, String freeFormTextSearch, List<String> selectedBuckets,
            String lookupIdColumn) {
        Tenant tenant = MultiTenantContext.getTenant();
        String ratingField = RatingEngine.toRatingAttrName(ratingEngine.getId(), RatingEngine.ScoreType.Rating);
        restrictNotNullSalesforceId = restrictNotNullSalesforceId == null ? false : restrictNotNullSalesforceId;

        DataPage cachedDataPage = null;
        try {
            String lookupIdColumnWithDefaultSfdcAccId = StringUtils.isBlank(lookupIdColumn)
                    ? InterfaceName.SalesforceAccountID.name() : lookupIdColumn;

            cachedDataPage = fetchData(tenant, ratingEngine, offset, maximum, entityType, sortBy, descending,
                    bucketFieldName, lookupFieldNames, restrictNotNullSalesforceId, freeFormTextSearch, selectedBuckets,
                    lookupIdColumnWithDefaultSfdcAccId, ratingField);
        } catch (Exception ex) {
            log.info("Ignoring exception and trying without lookup Id now", ex);
            cachedDataPage = fetchData(tenant, ratingEngine, offset, maximum, entityType, sortBy, descending,
                    bucketFieldName, lookupFieldNames, restrictNotNullSalesforceId, freeFormTextSearch, selectedBuckets,
                    null, ratingField);
        }

        DataPage resultDataPage = cachedDataPage;

        if (entityType == BusinessEntity.Account && StringUtils.isNotBlank(bucketFieldName)) {
            resultDataPage = handleBucketFieldName(cachedDataPage, ratingField, bucketFieldName);
        }

        log.info(String.format("Got # %d elements", resultDataPage.getData().size()));
        return resultDataPage;

    }

    private DataPage fetchData(Tenant tenant, RatingEngine ratingEngine, long offset, long maximum,
            BusinessEntity entityType, String sortBy, boolean descending, String bucketFieldName,
            List<String> lookupFieldNames, Boolean restrictNotNullSalesforceId, String freeFormTextSearch,
            List<String> selectedBuckets, String lookupIdColumn, String ratingField) {
        FrontEndQuery entityFrontEndQuery = createBasicFronEndQuery(ratingEngine, entityType,
                restrictNotNullSalesforceId, freeFormTextSearch, selectedBuckets, ratingField, lookupIdColumn);

        entityFrontEndQuery.setPageFilter(new PageFilter(offset, maximum));

        setSortField(entityType, sortBy, descending, entityFrontEndQuery);

        setLookups(entityType, entityFrontEndQuery, ratingField, lookupFieldNames);

        log.info(String.format("Entity query => %s", JsonUtils.serialize(entityFrontEndQuery)));

        DataPage cachedDataPage = entityProxy.getData( //
                tenant.getId(), //
                entityFrontEndQuery);
        return cachedDataPage;
    }

    @Override
    public Long getEntityPreviewCount(RatingEngine ratingEngine, BusinessEntity entityType,
            Boolean restrictNotNullSalesforceId, String freeFormTextSearch, List<String> selectedBuckets,
            String lookupIdColumn) {
        restrictNotNullSalesforceId = restrictNotNullSalesforceId == null ? false : restrictNotNullSalesforceId;
        selectedBuckets = selectedBuckets == null
                ? Arrays.asList(RatingBucketName.values()).stream().map(b -> b.getName()).collect(Collectors.toList())
                : selectedBuckets;

        Tenant tenant = MultiTenantContext.getTenant();
        String ratingField = RatingEngine.toRatingAttrName(ratingEngine.getId(), RatingEngine.ScoreType.Rating);
        Long count = null;

        try {
            String lookupIdColumnWithDefaultSfdcAccId = StringUtils.isBlank(lookupIdColumn)
                    ? InterfaceName.SalesforceAccountID.name() : lookupIdColumn;

            FrontEndQuery entityFrontEndQuery = createBasicFronEndQuery(ratingEngine, entityType,
                    restrictNotNullSalesforceId, freeFormTextSearch, selectedBuckets, ratingField,
                    lookupIdColumnWithDefaultSfdcAccId);

            count = entityProxy.getCount( //
                    tenant.getId(), //
                    entityFrontEndQuery);
            log.info(String.format("Entity query => %s, count = %s", JsonUtils.serialize(entityFrontEndQuery), count));
        } catch (Exception ex) {
            log.info("Ignoring exception and trying without lookup Id now", ex);
            FrontEndQuery entityFrontEndQuery = createBasicFronEndQuery(ratingEngine, entityType,
                    restrictNotNullSalesforceId, freeFormTextSearch, selectedBuckets, ratingField, null);

            count = entityProxy.getCount( //
                    tenant.getId(), //
                    entityFrontEndQuery);
            log.info(String.format("Entity query => %s, count = %s", JsonUtils.serialize(entityFrontEndQuery), count));
        }

        return count == null ? 0L : count;
    }

    private FrontEndQuery createBasicFronEndQuery(RatingEngine ratingEngine, BusinessEntity entityType,
            boolean restrictNotNullSalesforceId, String freeFormTextSearch, List<String> selectedBuckets,
            String ratingField, String lookupIdColumn) {
        FrontEndQuery entityFrontEndQuery = new FrontEndQuery();
        setBasicInfo(ratingEngine, entityType, entityFrontEndQuery, restrictNotNullSalesforceId, freeFormTextSearch,
                lookupIdColumn);
        setSelectedBuckets(entityFrontEndQuery, selectedBuckets, ratingField);
        return entityFrontEndQuery;
    }

    @VisibleForTesting
    void setSelectedBuckets(FrontEndQuery entityFrontEndQuery, List<String> selectedBuckets, String ratingField) {
        if (CollectionUtils.isNotEmpty(selectedBuckets)) {
            log.info(String.format("Only get data for the buckets selected: %s",
                    Arrays.toString(selectedBuckets.toArray())));
            Restriction originalRestriction = entityFrontEndQuery.getAccountRestriction().getRestriction();
            Restriction selectedBucketRestriction = Restriction.builder().let(BusinessEntity.Rating, ratingField)
                    .inCollection(Arrays.asList(selectedBuckets.toArray())).build();
            Restriction compoundRestriction = Restriction.builder().and(originalRestriction, selectedBucketRestriction)
                    .build();
            entityFrontEndQuery.getAccountRestriction().setRestriction(compoundRestriction);
        }
    }

    private void setBasicInfo(RatingEngine ratingEngine, BusinessEntity entityType, FrontEndQuery entityFrontEndQuery,
            boolean restrictNotNullSalesforceId, String freeFormTextSearch, String lookupIdColumn) {
        entityFrontEndQuery.setMainEntity(entityType);

        if (ratingEngine.getSegment().getAccountFrontEndRestriction() == null) {
            entityFrontEndQuery
                    .setAccountRestriction(new FrontEndRestriction(ratingEngine.getSegment().getAccountRestriction()));
        } else {
            entityFrontEndQuery.setAccountRestriction(ratingEngine.getSegment().getAccountFrontEndRestriction());
        }

        if (ratingEngine.getSegment().getContactFrontEndRestriction() == null) {
            entityFrontEndQuery
                    .setContactRestriction(new FrontEndRestriction(ratingEngine.getSegment().getContactRestriction()));
        } else {
            entityFrontEndQuery.setContactRestriction(ratingEngine.getSegment().getContactFrontEndRestriction());
        }

        if (entityType == BusinessEntity.Account) {
            Restriction accRestriction = entityFrontEndQuery.getAccountRestriction().getRestriction();
            Restriction effectiveRestriction = accRestriction;
            if (lookupIdColumn != null && restrictNotNullSalesforceId) {
                Restriction restrictionForNonNullLookupId = Restriction.builder().let(entityType, lookupIdColumn)
                        .isNotNull().build();
                effectiveRestriction = Restriction.builder().and(accRestriction, restrictionForNonNullLookupId).build();
            }
            entityFrontEndQuery.getAccountRestriction().setRestriction(effectiveRestriction);
        } else {
            entityFrontEndQuery.setRestrictNotNullSalesforceId(restrictNotNullSalesforceId);
        }
        entityFrontEndQuery.setFreeFormTextSearch(freeFormTextSearch);
    }

    private void setSortField(BusinessEntity entityType, String sortBy, boolean descending,
            FrontEndQuery entityFrontEndQuery) {
        List<AttributeLookup> lookups = new ArrayList<>();
        AttributeLookup attrLookup = new AttributeLookup(entityType,
                // StringUtils.isNotBlank(sortBy) ? sortBy :
                // InterfaceName.LastModifiedDate.name());
                StringUtils.isNotBlank(sortBy) ? sortBy : InterfaceName.AccountId.name());
        lookups.add(attrLookup);

        FrontEndSort sort = new FrontEndSort(lookups, descending);
        entityFrontEndQuery.setSort(sort);
    }

    private void setLookups(BusinessEntity entityType, FrontEndQuery entityFrontEndQuery, String ratingField,
            List<String> lookupFieldNames) {
        String[] fieldArray = null;
        if (CollectionUtils.isNotEmpty(lookupFieldNames)) {
            fieldArray = new String[lookupFieldNames.size()];
            lookupFieldNames.toArray(fieldArray);
        } else {
            if (entityType == BusinessEntity.Account) {
                fieldArray = new String[accountFields.size()];
                accountFields.toArray(fieldArray);
            } else {
                fieldArray = new String[contactFields.size()];
                contactFields.toArray(fieldArray);
            }
        }
        entityFrontEndQuery.addLookups(entityType, fieldArray);

        if (entityType == BusinessEntity.Account) {
            entityFrontEndQuery.addLookups(BusinessEntity.Rating, ratingField);
        }
    }

    private DataPage handleBucketFieldName(DataPage cachedDataPage, String ratingField, String bucketFieldName) {
        DataPage resultDataPage = new DataPage();

        List<Map<String, Object>> cachedEntityList = cachedDataPage.getData();
        List<Map<String, Object>> resultEntityList = null;

        if (CollectionUtils.isNotEmpty(cachedEntityList)) {
            resultEntityList = //
                    cachedEntityList //
                            .stream() //
                            .map( //
                                    cachedEntity -> {
                                        // clone cache map entity
                                        Map<String, Object> resultMapEntity = new HashMap<>(cachedEntity);
                                        // then replace bucket field name
                                        replaceScoreBucketFieldName(resultMapEntity, ratingField, bucketFieldName);
                                        return resultMapEntity;
                                    }) //
                            .collect(Collectors.toList());
        } else {
            resultEntityList = new ArrayList<>();
        }

        resultDataPage.setData(resultEntityList);

        return resultDataPage;
    }

    private void replaceScoreBucketFieldName(Map<String, Object> entity, String modelId, String bucketFieldName) {
        entity.put(bucketFieldName, entity.get(modelId));
        entity.remove(modelId);
    }

    @VisibleForTesting
    void setEntityProxy(EntityProxy entityProxy) {
        this.entityProxy = entityProxy;
    }
}
