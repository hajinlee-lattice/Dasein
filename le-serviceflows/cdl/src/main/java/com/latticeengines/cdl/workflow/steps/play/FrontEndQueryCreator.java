package com.latticeengines.cdl.workflow.steps.play;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
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

@Component
public class FrontEndQueryCreator {

    private static final Logger log = LoggerFactory.getLogger(FrontEndQueryCreator.class);

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private Map<BusinessEntity, List<String>> accountLookupFields;

    private Map<BusinessEntity, List<String>> contactLookupFields;

    @PostConstruct
    public void init() {

        initLookupFieldsConfiguration();
    }

    public void prepareFrontEndQueries(PlayLaunchContext playLaunchContext) {
        Play play = playLaunchContext.getPlay();
        FrontEndQuery accountFrontEndQuery = playLaunchContext.getAccountFrontEndQuery();
        FrontEndQuery contactFrontEndQuery = playLaunchContext.getContactFrontEndQuery();

        accountFrontEndQuery.setMainEntity(BusinessEntity.Account);
        contactFrontEndQuery.setMainEntity(BusinessEntity.Contact);

        prepareLookupsForFrontEndQueries(accountFrontEndQuery, contactFrontEndQuery);

        RatingEngine ratingEngine = play.getRatingEngine();

        if (ratingEngine != null) {
            prepareQueryUsingRatingsDefn(playLaunchContext);
        } else {
            throw new NullPointerException(String.format("Rating Engine is not set for the play %s", play.getName()));
        }

        accountFrontEndQuery.setRestrictNotNullSalesforceId(play.getExcludeItemsWithoutSalesforceId());
        contactFrontEndQuery.setRestrictNotNullSalesforceId(play.getExcludeItemsWithoutSalesforceId());

        setSortField(BusinessEntity.Account, Arrays.asList(InterfaceName.AccountId.name()), false,
                accountFrontEndQuery);

        setSortField(BusinessEntity.Contact, Arrays.asList(InterfaceName.ContactId.name()), false,
                contactFrontEndQuery);
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

    private void setSortField(BusinessEntity entityType, List<String> sortBy, boolean descending,
            FrontEndQuery entityFrontEndQuery) {
        if (CollectionUtils.isEmpty(sortBy)) {
            sortBy = Arrays.asList(InterfaceName.AccountId.name());
        }

        List<AttributeLookup> lookups = sortBy.stream() //
                .map(sort -> new AttributeLookup(entityType, sort)) //
                .collect(Collectors.toList());

        FrontEndSort sort = new FrontEndSort(lookups, descending);
        entityFrontEndQuery.setSort(sort);
    }

    private void prepareQueryUsingRatingsDefn(PlayLaunchContext playLaunchContext) {

        Play play = playLaunchContext.getPlay();
        FrontEndQuery accountFrontEndQuery = playLaunchContext.getAccountFrontEndQuery();
        FrontEndQuery contactFrontEndQuery = playLaunchContext.getContactFrontEndQuery();
        List<Object> modifiableAccountIdCollectionForContacts = playLaunchContext
                .getModifiableAccountIdCollectionForContacts();
        String modelId = playLaunchContext.getModelId();
        RatingEngine ratingEngine = play.getRatingEngine();

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

        List<RatingModel> ratingModels = new ArrayList<>();
        for (RatingModel model : ratingEngine.getRatingModels()) {
            ratingModels.add(model);
            break;
        }
        accountFrontEndQuery.setRatingModels(ratingModels);

        List<Lookup> lookups = accountFrontEndQuery.getLookups();
        Lookup lookup = new AttributeLookup(BusinessEntity.Rating, modelId);
        lookups.add(lookup);
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
                        InterfaceName.SalesforceAccountID.name(), //
                        InterfaceName.CompanyName.name(), //
                        InterfaceName.LDC_Name.name()));

        contactLookupFields = new HashMap<>();
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

}
