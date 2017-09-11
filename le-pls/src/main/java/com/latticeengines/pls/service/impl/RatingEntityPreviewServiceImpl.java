package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
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
            InterfaceName.SalesforceAccountID.name(), //
            InterfaceName.LastModifiedDate.name(), //
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
            boolean restrictNotNullSalesforceId) {
        Tenant tenent = MultiTenantContext.getTenant();

        FrontEndQuery entityFrontEndQuery = new FrontEndQuery();
        setBasicInfo(ratingEngine, entityType, entityFrontEndQuery, restrictNotNullSalesforceId);

        entityFrontEndQuery.setPageFilter(new PageFilter(offset, maximum));

        setSortField(entityType, sortBy, descending, entityFrontEndQuery);

        RatingModel model = setModelInfo(ratingEngine, entityFrontEndQuery);

        setLookups(entityType, entityFrontEndQuery, model, lookupFieldNames);

        log.info(String.format("Entity query => %s", JsonUtils.serialize(entityFrontEndQuery)));

        DataPage dataPage = entityProxy.getData( //
                tenent.getId(), //
                entityFrontEndQuery);

        if (entityType == BusinessEntity.Account && StringUtils.isNotBlank(bucketFieldName)) {
            postProcessDataPage(dataPage, model.getId(), bucketFieldName);
        }

        log.info(String.format("Got # %d elements", dataPage.getData().size()));
        return dataPage;

    }

    private void setBasicInfo(RatingEngine ratingEngine, BusinessEntity entityType, FrontEndQuery entityFrontEndQuery,
            boolean restrictNotNullSalesforceId) {
        entityFrontEndQuery.setMainEntity(entityType);

        entityFrontEndQuery.setAccountRestriction(ratingEngine.getSegment().getAccountFrontEndRestriction());
        entityFrontEndQuery.setContactRestriction(ratingEngine.getSegment().getContactFrontEndRestriction());

        entityFrontEndQuery.setRestrictNotNullSalesforceId(restrictNotNullSalesforceId);
    }

    private void setSortField(BusinessEntity entityType, String sortBy, boolean descending,
            FrontEndQuery entityFrontEndQuery) {
        List<AttributeLookup> lookups = new ArrayList<>();
        AttributeLookup attrLookup = new AttributeLookup(entityType,
                StringUtils.isNotBlank(sortBy) ? sortBy : InterfaceName.LastModifiedDate.name());
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

    private void postProcessDataPage(DataPage dataPage, String modelId, String bucketFieldName) {
        List<Map<String, Object>> entityList = dataPage.getData();
        if (CollectionUtils.isNotEmpty(entityList)) {
            entityList //
                    .stream().parallel() //
                    .forEach( //
                            entity -> //
                            replaceScoreBucketFieldName(entity, modelId, bucketFieldName));
        }
    }

    private void replaceScoreBucketFieldName(Map<String, Object> entity, String modelId, String bucketFieldName) {
        entity.put(bucketFieldName, entity.get(modelId));
        entity.remove(modelId);
    }

}
