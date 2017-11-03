package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.RatingEntityPreviewService;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public class RatingEntityPreviewServiceImpl implements RatingEntityPreviewService {

    private static final Logger log = LoggerFactory.getLogger(RatingEntityPreviewServiceImpl.class);

    @Autowired
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
            String sortBy, boolean descending, String bucketFieldName, List<String> lookupFieldNames,
            boolean restrictNotNullSalesforceId, String freeFormTextSearch, List<String> selectedBuckets) {
        Tenant tenent = MultiTenantContext.getTenant();

        FrontEndQuery entityFrontEndQuery = new FrontEndQuery();
        setBasicInfo(ratingEngine, entityType, entityFrontEndQuery, restrictNotNullSalesforceId, freeFormTextSearch);

        entityFrontEndQuery.setPageFilter(new PageFilter(offset, maximum));

        setSortField(entityType, sortBy, descending, entityFrontEndQuery);

        RatingModel model = setModelInfo(ratingEngine, entityFrontEndQuery);

        setLookups(entityType, entityFrontEndQuery, model, lookupFieldNames);

        setSelectedBuckets(entityFrontEndQuery, selectedBuckets, model);

        log.info(String.format("Entity query => %s", JsonUtils.serialize(entityFrontEndQuery)));

        DataPage cachedDataPage = entityProxy.getData( //
                tenent.getId(), //
                entityFrontEndQuery);

        DataPage resultDataPage = cachedDataPage;

        if (entityType == BusinessEntity.Account && StringUtils.isNotBlank(bucketFieldName)) {
            resultDataPage = handleBucketFieldName(cachedDataPage, model.getId(), bucketFieldName);
        }

        log.info(String.format("Got # %d elements", resultDataPage.getData().size()));
        return resultDataPage;

    }

    @VisibleForTesting
    void setSelectedBuckets(FrontEndQuery entityFrontEndQuery, List<String> selectedBuckets, RatingModel model) {
        if (CollectionUtils.isNotEmpty(selectedBuckets)) {
            log.info(String.format("Only get data for the buckets selected: %s",
                    Arrays.toString(selectedBuckets.toArray())));
            Restriction originalRestriction = entityFrontEndQuery.getAccountRestriction().getRestriction();
            Restriction selectedBucketRestriction = Restriction.builder().let(BusinessEntity.Rating, model.getId())
                    .inCollection(Arrays.asList(selectedBuckets.toArray())).build();
            Restriction compoundRestriction = Restriction.builder().and(originalRestriction, selectedBucketRestriction)
                    .build();
            entityFrontEndQuery.getAccountRestriction().setRestriction(compoundRestriction);
        }
    }

    private void setBasicInfo(RatingEngine ratingEngine, BusinessEntity entityType, FrontEndQuery entityFrontEndQuery,
            boolean restrictNotNullSalesforceId, String freeFormTextSearch) {
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

        entityFrontEndQuery.setRestrictNotNullSalesforceId(restrictNotNullSalesforceId);
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

    private void setLookups(BusinessEntity entityType, FrontEndQuery entityFrontEndQuery, RatingModel model,
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
            entityFrontEndQuery.addLookups(BusinessEntity.Rating, model.getId());
        }
    }

    private RatingModel setModelInfo(RatingEngine ratingEngine, FrontEndQuery entityFrontEndQuery) {
        List<RatingModel> ratingModels = new ArrayList<>();
        // todo - anoop -this is only valid for rule based model, make it more
        // generic
        RatingModel model = ratingEngine.getRatingModels().iterator().next();
        ratingModels.add(model);
        entityFrontEndQuery.setRatingModels(ratingModels);
        return model;
    }

    private DataPage handleBucketFieldName(DataPage cachedDataPage, String modelId, String bucketFieldName) {
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
                                        Map<String, Object> resultMapEntity = new HashMap<>();
                                        resultMapEntity.putAll(cachedEntity);
                                        // then replace bucket field name
                                        replaceScoreBucketFieldName(resultMapEntity, modelId, bucketFieldName);
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
