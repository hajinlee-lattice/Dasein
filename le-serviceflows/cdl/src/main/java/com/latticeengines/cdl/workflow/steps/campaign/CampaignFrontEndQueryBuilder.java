package com.latticeengines.cdl.workflow.steps.campaign;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

    protected static class Builder {

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

        public Builder limit(long limit) {
            queryBuilder.limit = limit;
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
        if (StringUtils.isNotBlank(ratingId) && CollectionUtils.isNotEmpty(bucketsToLaunch)) {
            addModelRatingBasedRestrictions();
        }
        addChannelSpecificRestrictions();
        if (isSuppressAccountsWithoutLookupId && StringUtils.isNotBlank(lookupId)) {
            suppressAccountsWithoutLookupId();
        }
        if (mainEntity == BusinessEntity.Contact && isSuppressContactsWithoutEmails) {
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
        if (mainEntity == BusinessEntity.Account && StringUtils.isNotBlank(lookupId)) {
            Restriction accountRestriction = campaignFrontEndQuery.getAccountRestriction().getRestriction();
            Restriction nonNullLookupIdRestriction = Restriction.builder().let(BusinessEntity.Account, lookupId)
                    .isNotNull().build();

            Restriction accountRestrictionWithNonNullLookupId = Restriction.builder()
                    .and(accountRestriction, nonNullLookupIdRestriction).build();
            campaignFrontEndQuery.getAccountRestriction().setRestriction(accountRestrictionWithNonNullLookupId);
        }
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
        if (mainEntity == BusinessEntity.Contact) {
            Restriction contactRestriction = campaignFrontEndQuery.getContactRestriction().getRestriction();
            Restriction nonNullLookupIdRestriction = Restriction.builder()
                    .let(BusinessEntity.Contact, InterfaceName.Email.name()).isNotNull().build();
            Restriction contactRestrictionWithNonNullEmail = Restriction.builder()
                    .and(contactRestriction, nonNullLookupIdRestriction).build();
            campaignFrontEndQuery.getContactRestriction().setRestriction(contactRestrictionWithNonNullEmail);
        }
    }

    private void setMainEntity() {
        campaignFrontEndQuery.setMainEntity(mainEntity);
    }

    private void setupLookups() {
        campaignFrontEndQuery.setLookups(accountLookups.stream()
                .map(cl -> new AttributeLookup(BusinessEntity.Account, cl)).collect(Collectors.toList()));
        if (mainEntity == BusinessEntity.Contact) {
            campaignFrontEndQuery.setLookups(contactLookups.stream()
                    .map(cl -> new AttributeLookup(BusinessEntity.Contact, cl)).collect(Collectors.toList()));
        }
    }

    private void setupBaseRestrictions() {
        campaignFrontEndQuery.setAccountRestriction(new FrontEndRestriction(baseAccountRestriction));
        baseContactRestriction = baseContactRestriction == null
                ? LogicalRestriction.builder().or(new ArrayList<>()).build()
                : baseContactRestriction;
        campaignFrontEndQuery.setContactRestriction(new FrontEndRestriction(baseContactRestriction));

    }

    private void addModelRatingBasedRestrictions() {
        if (mainEntity == BusinessEntity.Account) {
            Restriction ratingRestriction;
            Lookup lhs = new AttributeLookup(BusinessEntity.Rating, ratingId);
            Collection<Object> allowedRatingsCollection = bucketsToLaunch.stream().map(RatingBucketName::getName)
                    .collect(Collectors.toList());
            Lookup rhs = new CollectionLookup(allowedRatingsCollection);
            ratingRestriction = new ConcreteRestriction(false, lhs, ComparisonType.IN_COLLECTION, rhs);

            ratingRestriction = launchUnScored //
                    ? Restriction.builder()
                            .or(ratingRestriction, new ConcreteRestriction(false, lhs, ComparisonType.IS_NULL, null))
                            .build()
                    : ratingRestriction;

            Restriction finalAccountRestriction = Restriction.builder().and(baseAccountRestriction, ratingRestriction)
                    .build();
            campaignFrontEndQuery.setAccountRestriction(new FrontEndRestriction(finalAccountRestriction));
        }
    }

    private void addChannelSpecificRestrictions() {
        if (CDLExternalSystemName.Marketo.equals(destinationSystemName)
                && campaignFrontEndQuery.getContactRestriction() != null) {
            Restriction newContactRestrictionForAccountQuery = applyEmailFilterToContactRestriction(
                    campaignFrontEndQuery.getContactRestriction().getRestriction());
            campaignFrontEndQuery.setContactRestriction(new FrontEndRestriction(newContactRestrictionForAccountQuery));
        }
    }

    private Restriction applyEmailFilterToContactRestriction(Restriction contactRestriction) {
        Restriction emailFilter = Restriction.builder().let(BusinessEntity.Contact, InterfaceName.Email.name())
                .isNotNull().build();
        return Restriction.builder().and(contactRestriction, emailFilter).build();
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
