package com.latticeengines.leadprioritization.workflow.steps.play;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.RestrictionBuilder;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

@Component
public class FrontEndQueryCreator {

    private static final Logger log = LoggerFactory.getLogger(FrontEndQueryCreator.class);

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    private Map<BusinessEntity, List<String>> accountLookupFields;

    private Map<BusinessEntity, List<String>> contactLookupFields;

    @PostConstruct
    public void init() {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);

        initLookupFieldsConfiguration();
    }

    public String prepareFrontEndQueries(CustomerSpace customerSpace, Play play, String segmentName,
            FrontEndQuery accountFrontEndQuery, FrontEndQuery contactFrontEndQuery,
            List<Object> modifiableAccountIdCollectionForContacts) throws JsonProcessingException {
        accountFrontEndQuery.setMainEntity(BusinessEntity.Account);
        contactFrontEndQuery.setMainEntity(BusinessEntity.Contact);

        prepareLookupsForFrontEndQueries(accountFrontEndQuery, contactFrontEndQuery);

        RatingEngine ratingEngine = play.getRatingEngine();
        String modelId = null;

        if (ratingEngine != null) {
            modelId = prepareQueryUsingRatingsDefn(accountFrontEndQuery, contactFrontEndQuery,
                    modifiableAccountIdCollectionForContacts, ratingEngine);
        } else {
            log.error(String.format("Rating Engine is not set for the play %s", play.getName()));
        }

        if (accountFrontEndQuery.getAccountRestriction() == null) {
            prepareQueryUsingSegmentDefn(customerSpace, segmentName, accountFrontEndQuery, contactFrontEndQuery,
                    modifiableAccountIdCollectionForContacts);
        }

        accountFrontEndQuery.setRestrictNotNullSalesforceId(play.getExcludeItemsWithoutSalesforceId());
        contactFrontEndQuery.setRestrictNotNullSalesforceId(play.getExcludeItemsWithoutSalesforceId());

        setSortField(BusinessEntity.Account, InterfaceName.LEAccountIDLong.name(), false, accountFrontEndQuery);
        setSortField(BusinessEntity.Contact, InterfaceName.ContactId.name(), false, contactFrontEndQuery);

        return modelId;
    }

    private void prepareLookupsForFrontEndQueries(FrontEndQuery accountFrontEndQuery,
            FrontEndQuery contactFrontEndQuery) {
        List<Lookup> accountLookups = new ArrayList<>();
        accountLookupFields //
                .keySet().stream() //
                .forEach( //
                        businessEntity -> prepareLookups(businessEntity, accountLookups, accountLookupFields));

        List<Lookup> contactLookups = new ArrayList<>();
        contactLookupFields //
                .keySet().stream() //
                .forEach( //
                        businessEntity -> prepareLookups(businessEntity, contactLookups, contactLookupFields));

        accountFrontEndQuery.setLookups(accountLookups);
        contactFrontEndQuery.setLookups(contactLookups);
    }

    private void prepareLookups(BusinessEntity businessEntity, List<Lookup> lookups,
            Map<BusinessEntity, List<String>> entityLookupFields) {
        entityLookupFields.get(businessEntity) //
                .stream() //
                .forEach( //
                        field -> lookups.add(new AttributeLookup(businessEntity, field)));
    }

    private void prepareQueryUsingSegmentDefn(CustomerSpace customerSpace, String segmentName,
            FrontEndQuery accountFrontEndQuery, FrontEndQuery contactFrontEndQuery,
            List<Object> modifiableAccountIdCollectionForContacts) throws JsonProcessingException {
        Restriction segmentRestriction = internalResourceRestApiProxy.getSegmentRestrictionQuery(customerSpace,
                segmentName);

        log.info(String.format("Processing restriction: %s", JsonUtils.serialize(segmentRestriction)));

        FrontEndRestriction frontEndRestriction = new FrontEndRestriction(segmentRestriction);
        accountFrontEndQuery.setAccountRestriction(frontEndRestriction);
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

    private String prepareQueryUsingRatingsDefn(FrontEndQuery accountFrontEndQuery, FrontEndQuery contactFrontEndQuery,
            List<Object> modifiableAccountIdCollectionForContacts, RatingEngine ratingEngine) {
        List<Lookup> lookups;
        if (ratingEngine.getSegment() != null) {
            FrontEndRestriction accountRestriction = new FrontEndRestriction(
                    ratingEngine.getSegment().getAccountRestriction());
            FrontEndRestriction contactRestriction = new FrontEndRestriction(
                    ratingEngine.getSegment().getContactRestriction());

            accountFrontEndQuery.setAccountRestriction(accountRestriction);
            accountFrontEndQuery.setContactRestriction(contactRestriction);

            Restriction extractedContactRestriction = contactRestriction == null ? new RestrictionBuilder().build()
                    : contactRestriction.getRestriction();
            contactFrontEndQuery.setContactRestriction(
                    prepareContactRestriction(extractedContactRestriction, modifiableAccountIdCollectionForContacts));
        }

        String modelId = null;
        List<RatingModel> ratingModels = new ArrayList<>();
        for (RatingModel model : ratingEngine.getRatingModels()) {
            ratingModels.add(model);
            modelId = model.getId();
            break;
        }
        accountFrontEndQuery.setRatingModels(ratingModels);

        lookups = accountFrontEndQuery.getLookups();
        Lookup lookup = new AttributeLookup(BusinessEntity.Rating, modelId);
        lookups.add(lookup);
        return modelId;
    }

    private FrontEndRestriction prepareContactRestriction(Restriction extractedContactRestriction,
            Collection<Object> modifiableAccountIdCollection) {
        Restriction accountIdRestriction = Restriction.builder()
                .let(BusinessEntity.Contact, InterfaceName.AccountId.name()).inCollection(modifiableAccountIdCollection)
                .build();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction(
                Restriction.builder().and(extractedContactRestriction, accountIdRestriction).build());

        return frontEndRestriction;
    }

    @VisibleForTesting
    void initLookupFieldsConfiguration() {
        accountLookupFields = new HashMap<>();
        accountLookupFields.put(BusinessEntity.Account,
                Arrays.asList(InterfaceName.AccountId.name(), //
                        InterfaceName.LEAccountIDLong.name(), //
                        InterfaceName.SalesforceAccountID.name(), //
                        InterfaceName.CompanyName.name(), //
                        InterfaceName.LDC_Name.name()));

        contactLookupFields = new HashMap<>();
        contactLookupFields.put(BusinessEntity.Account, Arrays.asList(InterfaceName.AccountId.name()));
        contactLookupFields.put(BusinessEntity.Contact,
                Arrays.asList(InterfaceName.AccountId.name(), //
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
                        InterfaceName.Address_Street_1.name()));
    }

    @VisibleForTesting
    void setInternalResourceRestApiProxy(InternalResourceRestApiProxy internalResourceRestApiProxy) {
        this.internalResourceRestApiProxy = internalResourceRestApiProxy;
    }

}
