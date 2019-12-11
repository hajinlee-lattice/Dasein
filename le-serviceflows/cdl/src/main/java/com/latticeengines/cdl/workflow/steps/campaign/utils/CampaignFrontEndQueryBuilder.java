package com.latticeengines.cdl.workflow.steps.campaign.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.CollectionLookup;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;

public class CampaignFrontEndQueryBuilder {

    private FrontEndQuery campaignFrontEndQuery;
    private BusinessEntity mainEntity;
    private CustomerSpace customerSpace;
    private String lookupId;
    private String ratingId;
    private Restriction baseAccountRestriction;
    private Restriction baseContactRestriction;
    private Set<RatingBucketName> bucketsToLaunch;
    private boolean launchUnScored;
    private long limit;
    private CDLExternalSystemName destinationSystemName;
    private boolean isSuppressAccountsWithoutLookupId;
    private boolean isSuppressAccountsWithoutContacts;
    private boolean isSuppressContactsWithoutEmails;

    private final List<String> accountLookups = Collections.singletonList(InterfaceName.AccountId.name());

    private final List<String> contactLookups = Arrays.asList(InterfaceName.ContactId.name(),
            InterfaceName.AccountId.name());

    public static class Builder {

        private CampaignFrontEndQueryBuilder queryBuilder = new CampaignFrontEndQueryBuilder();

        public Builder customerSpace(CustomerSpace customerSpace) {
            queryBuilder.customerSpace = customerSpace;
            return this;
        }

        public Builder mainEntity(BusinessEntity mainEntity) {
            queryBuilder.mainEntity = mainEntity;
            return this;
        }

        public Builder lookupId(String lookupId) {
            queryBuilder.lookupId = StringUtils.isNotBlank(lookupId) ? lookupId.trim() : lookupId;
            return this;
        }

        public Builder ratingId(String ratingId) {
            queryBuilder.ratingId = ratingId;
            return this;
        }

        public Builder baseAccountRestriction(Restriction baseAccountRestriction) {
            queryBuilder.baseAccountRestriction = baseAccountRestriction;
            return this;
        }

        public Builder baseContactRestriction(Restriction baseContactRestriction) {
            queryBuilder.baseContactRestriction = baseContactRestriction;
            return this;
        }

        public Builder bucketsToLaunch(Set<RatingBucketName> bucketsToLaunch) {
            queryBuilder.bucketsToLaunch = bucketsToLaunch;
            return this;
        }

        public Builder launchUnScored(boolean launchUnscored) {
            queryBuilder.launchUnScored = launchUnscored;
            return this;
        }

        public Builder destinationSystemName(CDLExternalSystemName destinationSystemName) {
            queryBuilder.destinationSystemName = destinationSystemName;
            return this;
        }

        public Builder isSuppressAccountsWithoutLookupId(boolean isSuppressAccountsWithoutLookupId) {
            queryBuilder.isSuppressAccountsWithoutLookupId = isSuppressAccountsWithoutLookupId;
            return this;
        }

        public Builder isSuppressContactsWithoutEmails(boolean isSuppressContactsWithoutEmails) {
            queryBuilder.isSuppressContactsWithoutEmails = isSuppressContactsWithoutEmails;
            return this;
        }

        public Builder isSuppressAccountsWithoutContacts(boolean isSuppressAccountsWithoutContacts) {
            queryBuilder.isSuppressAccountsWithoutContacts = isSuppressAccountsWithoutContacts;
            return this;
        }

        public Builder limit(Long limit) {
            if (limit != null) {
                queryBuilder.limit = limit;
            } else {
                queryBuilder.limit = 0L;
            }

            return this;
        }

        public CampaignFrontEndQueryBuilder getCampaignFrontEndQueryBuilder() {
            return queryBuilder;
        }

    }

    public final FrontEndQuery build() {
        campaignFrontEndQuery = new FrontEndQuery();
        setMainEntity();
        setupLookups();
        setupBaseRestrictions();
        if (StringUtils.isNotBlank(ratingId)) {
            addModelRatingBasedRestrictions();
        }
        if (isSuppressAccountsWithoutLookupId && StringUtils.isNotBlank(lookupId)) {
            suppressAccountsWithoutLookupId();
        }
        if (isSuppressContactsWithoutEmails) {
            filterContactsWithoutEmail();
        }
        addSort();
        setLimit();
        if (mainEntity == BusinessEntity.Contact && isSuppressAccountsWithoutContacts) {
            filterAccountsWithoutContacts();
        }
        return campaignFrontEndQuery;
    }

    private void setLimit() {
        if (limit > 0) {
            campaignFrontEndQuery.setPageFilter(new PageFilter(0, limit));
        }
    }

    private void suppressAccountsWithoutLookupId() {
        Restriction accountRestriction = campaignFrontEndQuery.getAccountRestriction().getRestriction();
        Restriction nonNullLookupIdRestriction = Restriction.builder().let(BusinessEntity.Account, lookupId).isNotNull()
                .build();

        Restriction accountRestrictionWithNonNullLookupId = Restriction.builder()
                .and(accountRestriction, nonNullLookupIdRestriction).build();
        campaignFrontEndQuery.getAccountRestriction().setRestriction(accountRestrictionWithNonNullLookupId);
    }

    private void filterAccountsWithoutContacts() {
        // Bit hacky approach, If FrontEndQuery's MainEntity is Contact, Accounts with
        // no contacts automatically get filtered.
        // So set FrontEndQuery's MainEntity to Account when filtering isn't needed
        // Also this is the reason, this should always be done right before returning
        // the front end query
        if (isSuppressAccountsWithoutContacts) {
            campaignFrontEndQuery.setMainEntity(BusinessEntity.Contact);
        } else {
            campaignFrontEndQuery.setMainEntity(BusinessEntity.Account);
        }
    }

    private void filterContactsWithoutEmail() {
        Restriction contactRestriction = campaignFrontEndQuery.getContactRestriction().getRestriction();
        Restriction nonNullEmailRestriction = Restriction.builder()
                .let(BusinessEntity.Contact, InterfaceName.Email.name()).isNotNull().build();
        Restriction contactRestrictionWithNonNullEmail = Restriction.builder()
                .and(contactRestriction, nonNullEmailRestriction).build();
        campaignFrontEndQuery.getContactRestriction().setRestriction(contactRestrictionWithNonNullEmail);
    }

    private void setMainEntity() {
        campaignFrontEndQuery.setMainEntity(mainEntity);
    }

    private void setupLookups() {
        List<Lookup> lookups = new ArrayList<>();
        lookups.add(new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name()));
        if (mainEntity == BusinessEntity.Contact) {
            lookups.add(new AttributeLookup(BusinessEntity.Contact, InterfaceName.ContactId.name()));
        }
        campaignFrontEndQuery.setLookups(lookups);
    }

    private void setupBaseRestrictions() {
        campaignFrontEndQuery.setAccountRestriction(new FrontEndRestriction(baseAccountRestriction));
        baseContactRestriction = baseContactRestriction == null
                ? LogicalRestriction.builder().or(new ArrayList<>()).build()
                : baseContactRestriction;
        campaignFrontEndQuery.setContactRestriction(new FrontEndRestriction(baseContactRestriction));

    }

    private void addModelRatingBasedRestrictions() {
        Lookup lhs = new AttributeLookup(BusinessEntity.Rating, ratingId);

        Restriction ratingBucketsRestriction = CollectionUtils.isNotEmpty(bucketsToLaunch)
                ? new ConcreteRestriction(false, lhs, ComparisonType.IN_COLLECTION,
                        new CollectionLookup(
                                bucketsToLaunch.stream().map(RatingBucketName::getName).collect(Collectors.toList())))
                : null;
        Restriction unScoredRestriction = launchUnScored
                ? new ConcreteRestriction(false, lhs, ComparisonType.IS_NULL, null)
                : null;

        Restriction ratingRestriction = Restriction.builder().or(ratingBucketsRestriction, unScoredRestriction).build();

        Restriction finalAccountRestriction = Restriction.builder()
                .and(campaignFrontEndQuery.getAccountRestriction().getRestriction(), ratingRestriction).build();
        campaignFrontEndQuery.setAccountRestriction(new FrontEndRestriction(finalAccountRestriction));
    }

    private void addSort() {

        if (ratingId != null) {
            setSortField(BusinessEntity.Rating, Collections.singletonList(ratingId), campaignFrontEndQuery);
        }
        setSortField(BusinessEntity.Account, Collections.singletonList(InterfaceName.AccountId.name()),
                campaignFrontEndQuery);

        if (mainEntity == BusinessEntity.Contact) {
            setSortField(BusinessEntity.Contact, Collections.singletonList(InterfaceName.ContactId.name()),
                    campaignFrontEndQuery);
        }
    }

    private void setSortField(BusinessEntity entityType, List<String> sortBy, FrontEndQuery entityFrontEndQuery) {
        if (CollectionUtils.isEmpty(sortBy)) {
            sortBy = Collections.singletonList(InterfaceName.AccountId.name());
        }

        List<AttributeLookup> lookups = sortBy.stream() //
                .map(sort -> new AttributeLookup(entityType, sort)) //
                .collect(Collectors.toList());

        FrontEndSort sort;
        FrontEndSort existingSort = entityFrontEndQuery.getSort();
        if (existingSort == null) {
            sort = new FrontEndSort(lookups, false);
        } else {
            List<AttributeLookup> combinedLookups = new ArrayList<>();
            combinedLookups.addAll(existingSort.getAttributes());
            combinedLookups.addAll(lookups);
            sort = new FrontEndSort(combinedLookups, false);
        }
        entityFrontEndQuery.setSort(sort);
    }
}
