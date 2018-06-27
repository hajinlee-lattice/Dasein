package com.latticeengines.cdl.workflow.steps.play;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.CollectionLookup;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.RestrictionBuilder;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;

@Component
public class FrontEndQueryCreator {

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private Map<BusinessEntity, List<String>> accountLookupFields;

    private Map<BusinessEntity, List<String>> contactLookupFields;

    @Value("${playmaker.workflow.contact.shouldapply.launch.flag.sfdcid.exclusion:true}")
    private Boolean applyExcludeItemsWithoutSalesforceIdOnContacts;

    @PostConstruct
    public void init() {

        initLookupFieldsConfiguration();
    }

    public void prepareFrontEndQueries(PlayLaunchContext playLaunchContext) {
        Play play = playLaunchContext.getPlay();
        PlayLaunch launch = playLaunchContext.getPlayLaunch();
        FrontEndQuery accountFrontEndQuery = playLaunchContext.getAccountFrontEndQuery();
        FrontEndQuery contactFrontEndQuery = playLaunchContext.getContactFrontEndQuery();

        accountFrontEndQuery.setMainEntity(BusinessEntity.Account);
        contactFrontEndQuery.setMainEntity(BusinessEntity.Contact);

        prepareLookupsForFrontEndQueries(accountFrontEndQuery, contactFrontEndQuery, launch.getDestinationAccountId());

        RatingEngine ratingEngine = play.getRatingEngine();

        if (ratingEngine != null) {
            prepareQueryUsingRatingsDefn(playLaunchContext);
        } else {
            throw new NullPointerException(String.format("Rating Engine is not set for the play %s", play.getName()));
        }

        if (applyExcludeItemsWithoutSalesforceIdOnContacts != Boolean.FALSE) {
            contactFrontEndQuery.setRestrictNotNullSalesforceId(launch.getExcludeItemsWithoutSalesforceId());
        }

        addSort(playLaunchContext, accountFrontEndQuery, contactFrontEndQuery);
    }

    private void addSort(PlayLaunchContext playLaunchContext, FrontEndQuery accountFrontEndQuery,
            FrontEndQuery contactFrontEndQuery) {
        String ratingId = playLaunchContext.getRatingId();

        setSortField(BusinessEntity.Rating, Arrays.asList(ratingId), false, accountFrontEndQuery);
        setSortField(BusinessEntity.Account, Arrays.asList(InterfaceName.AccountId.name()), false,
                accountFrontEndQuery);
        setSortField(BusinessEntity.Contact, Arrays.asList(InterfaceName.ContactId.name()), false,
                contactFrontEndQuery);
    }

    private void prepareLookupsForFrontEndQueries(FrontEndQuery accountFrontEndQuery,
            FrontEndQuery contactFrontEndQuery, String destinationAccountId) {
        Map<BusinessEntity, List<String>> tempAccLookupFields;
        if (StringUtils.isBlank(destinationAccountId)) {
            tempAccLookupFields = accountLookupFields;
        } else {
            final String fDestinationAccountId = destinationAccountId.trim();

            tempAccLookupFields = new HashMap<>();
            List<String> colList = accountLookupFields.get(BusinessEntity.Account).stream()
                    .filter(c -> !fDestinationAccountId.equals(c)) //
                    .collect(Collectors.toList());
            colList.add(fDestinationAccountId);
            tempAccLookupFields.put(BusinessEntity.Account, colList);
        }
        final Map<BusinessEntity, List<String>> accLookupFields = tempAccLookupFields;
        List<Lookup> accountLookups = new ArrayList<>();
        accountLookupFields //
                .keySet().stream() //
                .forEach( //
                        businessEntity -> prepareLookups(businessEntity, accountLookups, accLookupFields));

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

        FrontEndSort sort;
        FrontEndSort existingSort = entityFrontEndQuery.getSort();
        if (existingSort == null) {
            sort = new FrontEndSort(lookups, descending);
        } else {
            List<AttributeLookup> combinedLookups = new ArrayList<>();
            combinedLookups.addAll(existingSort.getAttributes());
            combinedLookups.addAll(lookups);
            sort = new FrontEndSort(combinedLookups, descending);
        }
        entityFrontEndQuery.setSort(sort);
    }

    private void prepareQueryUsingRatingsDefn(PlayLaunchContext playLaunchContext) {

        Play play = playLaunchContext.getPlay();
        FrontEndQuery accountFrontEndQuery = playLaunchContext.getAccountFrontEndQuery();
        FrontEndQuery contactFrontEndQuery = playLaunchContext.getContactFrontEndQuery();
        List<Object> modifiableAccountIdCollectionForContacts = playLaunchContext
                .getModifiableAccountIdCollectionForContacts();
        String ratingId = playLaunchContext.getRatingId();
        RatingEngine ratingEngine = play.getRatingEngine();

        if (ratingEngine.getSegment() != null) {
            FrontEndRestriction accountRestriction = createAccountQueryWithAllowedRating(playLaunchContext, ratingId,
                    ratingEngine);

            FrontEndRestriction contactRestriction = new FrontEndRestriction(
                    ratingEngine.getSegment().getContactRestriction());

            accountFrontEndQuery.setAccountRestriction(accountRestriction);
            accountFrontEndQuery.setContactRestriction(contactRestriction);

            Restriction extractedContactRestriction = contactRestriction == null ? new RestrictionBuilder().build()
                    : contactRestriction.getRestriction();
            contactFrontEndQuery.setContactRestriction(
                    prepareContactRestriction(extractedContactRestriction, modifiableAccountIdCollectionForContacts));
        }

        List<RatingModel> ratingModels = Collections.singletonList(playLaunchContext.getPublishedIteration());
        accountFrontEndQuery.setRatingModels(ratingModels);

        // TODO add filtering based on list of selected buckets and update
        // selected vs skipped count accordingly
        List<Lookup> lookups = accountFrontEndQuery.getLookups();
        Lookup lookup = new AttributeLookup(BusinessEntity.Rating, ratingId);
        lookups.add(lookup);
    }

    private FrontEndRestriction createAccountQueryWithAllowedRating(PlayLaunchContext playLaunchContext,
            String ratingId, RatingEngine ratingEngine) {
        Lookup lhs = new AttributeLookup(BusinessEntity.Rating, ratingId);
        Collection<Object> allowedRatingsCollection = getAllowedRatingsCollection(
                playLaunchContext.getPlayLaunch().getBucketsToLaunch());
        Lookup rhs = new CollectionLookup(allowedRatingsCollection);
        Restriction allowedRatingRestriction = new ConcreteRestriction(false, lhs, ComparisonType.IN_COLLECTION, rhs);
        Restriction finalAccountRestriction = new LogicalRestriction(LogicalOperator.AND,
                Arrays.asList(ratingEngine.getSegment().getAccountRestriction(), allowedRatingRestriction));
        return new FrontEndRestriction(finalAccountRestriction);
    }

    private Collection<Object> getAllowedRatingsCollection(Set<RatingBucketName> allowedRatings) {
        Collection<Object> coll = new ArrayList<>();
        allowedRatings.stream().forEach(rating -> coll.add(rating.getName()));
        return coll;
    }

    private FrontEndRestriction prepareContactRestriction(Restriction extractedContactRestriction,
            Collection<Object> modifiableAccountIdCollection) {
        Restriction accountIdRestriction = Restriction.builder()
                .let(BusinessEntity.Contact, InterfaceName.AccountId.name()).inCollection(modifiableAccountIdCollection)
                .build();
        return new FrontEndRestriction(
                Restriction.builder().and(extractedContactRestriction, accountIdRestriction).build());
    }

    @VisibleForTesting
    void initLookupFieldsConfiguration() {
        accountLookupFields = new HashMap<>();
        accountLookupFields.put(BusinessEntity.Account,
                Arrays.asList(InterfaceName.AccountId.name(), //
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
