package com.latticeengines.cdl.workflow.steps.campaign;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private Restriction baseRestriction;
    private Set<RatingBucketName> bucketsToLaunch;
    private boolean launchUnscored;
    private CDLExternalSystemName destinationSystemName;
    private boolean isSupressAccountWithoutLookupId;

    private final List<String> accountLookups = Stream
            .of(InterfaceName.AccountId.name(), InterfaceName.CustomerAccountId.name()).collect(Collectors.toList());

    private final List<String> contactLookups = Stream
            .of(InterfaceName.ContactId.name(), InterfaceName.CustomerContactId.name(), InterfaceName.AccountId.name())
            .collect(Collectors.toList());

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

        public Builder targetSegmentRestriction(Restriction targetSegmentRestriction) {
            queryBuilder.baseRestriction = targetSegmentRestriction;
            return this;
        }

        public Builder bucketsToLaunch(Set<RatingBucketName> bucketsToLaunch) {
            queryBuilder.bucketsToLaunch = bucketsToLaunch;
            return this;
        }

        public Builder launchUnscored(boolean launchUnscored) {
            queryBuilder.launchUnscored = launchUnscored;
            return this;
        }

        public Builder destinationSystemName(CDLExternalSystemName destinationSystemName) {
            queryBuilder.destinationSystemName = destinationSystemName;
            return this;
        }

        public Builder isSupressAccountWithoutLookupId(boolean isSupressAccountWithoutLookupId) {
            queryBuilder.isSupressAccountWithoutLookupId = isSupressAccountWithoutLookupId;
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
        if (isSupressAccountWithoutLookupId) {
            addLookupIdBasedSuppression();
        }
        addSort();
        return campaignFrontEndQuery;
    }

    private void addLookupIdBasedSuppression() {
        if (mainEntity == BusinessEntity.Account && StringUtils.isNotBlank(lookupId)) {
            Restriction accountRestriction = campaignFrontEndQuery.getAccountRestriction().getRestriction();
            Restriction nonNullLookupIdRestriction = Restriction.builder().let(BusinessEntity.Account, lookupId)
                    .isNotNull().build();

            Restriction accountRestrictionWithNonNullLookupId = Restriction.builder()
                    .and(accountRestriction, nonNullLookupIdRestriction).build();
            campaignFrontEndQuery.getAccountRestriction().setRestriction(accountRestrictionWithNonNullLookupId);
        }
    }

    private void setMainEntity() {
        campaignFrontEndQuery.setMainEntity(mainEntity);
    }

    private void setupLookups() {
        if (mainEntity == BusinessEntity.Contact) {
            campaignFrontEndQuery.setLookups(contactLookups.stream()
                    .map(cl -> new AttributeLookup(BusinessEntity.Contact, cl)).collect(Collectors.toList()));
        }
        if (mainEntity == BusinessEntity.Account) {
            if (StringUtils.isNotBlank(lookupId)) {
                accountLookups.add(lookupId);
            }
            campaignFrontEndQuery.setLookups(accountLookups.stream()
                    .map(cl -> new AttributeLookup(BusinessEntity.Contact, cl)).collect(Collectors.toList()));
        }
    }

    private void setupBaseRestrictions() {
        if (mainEntity == BusinessEntity.Account) {
            campaignFrontEndQuery.setAccountRestriction(new FrontEndRestriction(baseRestriction));
        }
        if (mainEntity == BusinessEntity.Contact) {
            baseRestriction = baseRestriction == null ? LogicalRestriction.builder().or(new ArrayList<>()).build()
                    : baseRestriction;
            campaignFrontEndQuery.setContactRestriction(new FrontEndRestriction(baseRestriction));
        }
    }

    private void addModelRatingBasedRestrictions() {
        if (mainEntity == BusinessEntity.Account) {
            Restriction ratingRestriction;
            Lookup lhs = new AttributeLookup(BusinessEntity.Rating, ratingId);
            Collection<Object> allowedRatingsCollection = bucketsToLaunch.stream().map(RatingBucketName::getName)
                    .collect(Collectors.toList());
            Lookup rhs = new CollectionLookup(allowedRatingsCollection);
            ratingRestriction = new ConcreteRestriction(false, lhs, ComparisonType.IN_COLLECTION, rhs);

            ratingRestriction = launchUnscored //
                    ? Restriction.builder()
                            .or(ratingRestriction, new ConcreteRestriction(false, lhs, ComparisonType.IS_NULL, null))
                            .build()
                    : ratingRestriction;

            Restriction finalAccountRestriction = Restriction.builder().and(baseRestriction, ratingRestriction).build();
            campaignFrontEndQuery.setAccountRestriction(new FrontEndRestriction(finalAccountRestriction));
        }
    }

    private void addChannelSpecificRestrictions() {
        if (CDLExternalSystemName.Marketo.equals(destinationSystemName)) {
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
        if (mainEntity == BusinessEntity.Account) {
            if (ratingId != null) {
                setSortField(BusinessEntity.Rating, Collections.singletonList(ratingId), campaignFrontEndQuery);
            }
            setSortField(BusinessEntity.Account, Collections.singletonList(InterfaceName.AccountId.name()),
                    campaignFrontEndQuery);
        }
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
