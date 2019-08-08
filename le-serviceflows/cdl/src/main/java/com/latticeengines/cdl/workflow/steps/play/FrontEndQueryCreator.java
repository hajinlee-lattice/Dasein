package com.latticeengines.cdl.workflow.steps.play;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
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

@Component
public class FrontEndQueryCreator {

    private static final Logger log = LoggerFactory.getLogger(FrontEndQueryCreator.class);

    @Value("${yarn.pls.url}")
    private String internalResourceHostPort;

    private Map<BusinessEntity, List<String>> accountLookupFields;

    private Map<BusinessEntity, List<String>> contactLookupFields;

    @Value("${playmaker.workflow.contact.shouldapply.launch.flag.sfdcid.exclusion:true}")
    private Boolean applyExcludeItemsWithoutSalesforceIdOnContacts;

    @PostConstruct
    public void init() {

        initLookupFieldsConfiguration();
    }

    public void prepareFrontEndQueries(PlayLaunchContext playLaunchContext, boolean useSpark) {
        PlayLaunch launch = playLaunchContext.getPlayLaunch();
        FrontEndQuery accountFrontEndQuery = playLaunchContext.getAccountFrontEndQuery();
        FrontEndQuery contactFrontEndQuery = playLaunchContext.getContactFrontEndQuery();

        accountFrontEndQuery.setMainEntity(BusinessEntity.Account);
        contactFrontEndQuery.setMainEntity(BusinessEntity.Contact);

        prepareLookupsForFrontEndQueries(playLaunchContext, useSpark);

        prepareQueryWithRestrictions(playLaunchContext, useSpark);

        if (applyExcludeItemsWithoutSalesforceIdOnContacts != Boolean.FALSE) {
            contactFrontEndQuery.setRestrictNotNullSalesforceId(launch.getExcludeItemsWithoutSalesforceId());
        }

        addSort(playLaunchContext, accountFrontEndQuery, contactFrontEndQuery);
    }

    public void prepareFrontEndQueries(PlayLaunchContext playLaunchContext) {
        prepareFrontEndQueries(playLaunchContext, false);
    }

    private void addSort(PlayLaunchContext playLaunchContext, FrontEndQuery accountFrontEndQuery,
            FrontEndQuery contactFrontEndQuery) {
        String ratingId = playLaunchContext.getRatingId();
        if (ratingId != null) {
            setSortField(BusinessEntity.Rating, Collections.singletonList(ratingId), false, accountFrontEndQuery);
        }
        setSortField(BusinessEntity.Account, Collections.singletonList(InterfaceName.AccountId.name()), false,
                accountFrontEndQuery);
        setSortField(BusinessEntity.Contact, Collections.singletonList(InterfaceName.ContactId.name()), false,
                contactFrontEndQuery);
    }

    private void prepareLookupsForFrontEndQueries(PlayLaunchContext playLaunchContext, boolean useSpark) {
        String destinationAccountId = playLaunchContext.getPlayLaunch().getDestinationAccountId();
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
        // if useSpark, need to union with user configured fields
        if (useSpark) {
            unionAccountAndContactLookups(accountLookups, contactLookups, playLaunchContext.getFieldMappingMetadata());
        }
        playLaunchContext.getAccountFrontEndQuery().setLookups(accountLookups);
        playLaunchContext.getContactFrontEndQuery().setLookups(contactLookups);
    }

    @VisibleForTesting
    void unionAccountAndContactLookups(List<Lookup> accountLookups, List<Lookup> contactLookups,
            List<ColumnMetadata> fieldMappingMetadata) {
        if (CollectionUtils.isNotEmpty(fieldMappingMetadata)) {
            Map<BusinessEntity, Set<Lookup>> fieldMaps = new HashMap<>();
            fieldMappingMetadata.stream().filter(md -> !md.isCampaignDerivedField()).forEach(md -> {
                BusinessEntity entity = md.getEntity();
                if (!fieldMaps.containsKey(entity)) {
                    fieldMaps.put(md.getEntity(), new HashSet<>());
                }
                fieldMaps.get(entity).add(new AttributeLookup(entity, md.getAttrName()));
            });
            // check to see if the entities are eligible for export
            if (!BusinessEntity.EXPORT_ENTITIES.containsAll(fieldMaps.keySet())) {
                throw new RuntimeException("Not every entity is eligible for export " + fieldMaps.keySet());
            }

            Iterator<Lookup> accountIterator = accountLookups.iterator();
            Iterator<Lookup> contactIterator = contactLookups.iterator();
            Set<Lookup> userConfiguredAccountLookups = fieldMaps.get(BusinessEntity.Account);
            Set<Lookup> userConfiguredContactLookups = fieldMaps.get(BusinessEntity.Contact);
            if (userConfiguredAccountLookups != null) {
                while (accountIterator.hasNext()) {
                    Lookup accLookup = accountIterator.next();
                    if (userConfiguredAccountLookups.contains(accLookup)) {
                        userConfiguredAccountLookups.remove(accLookup);
                    }
                }
                accountLookups.addAll(userConfiguredAccountLookups);
                fieldMaps.remove(BusinessEntity.Account);
            }
            if (userConfiguredContactLookups != null) {
                while (contactIterator.hasNext()) {
                    Lookup conLookup = contactIterator.next();
                    if (userConfiguredContactLookups.contains(conLookup)) {
                        userConfiguredContactLookups.remove(conLookup);
                    }
                }
                contactLookups.addAll(userConfiguredContactLookups);
                fieldMaps.remove(BusinessEntity.Contact);
            }
            // add remaining lookups to AccountLookups
            if (MapUtils.isNotEmpty(fieldMaps)) {
                fieldMaps.entrySet().iterator().forEachRemaining(entry -> {
                    accountLookups.addAll(entry.getValue());
                });
            }
            log.info("accountLookups=" + Arrays.toString(accountLookups.toArray()));
            log.info("contactLookups=" + Arrays.toString(contactLookups.toArray()));
        }
    }

    @SuppressWarnings("unused")
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
            sortBy = Collections.singletonList(InterfaceName.AccountId.name());
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

    private void prepareQueryWithRestrictions(PlayLaunchContext playLaunchContext, boolean useSpark) {

        FrontEndQuery accountFrontEndQuery = playLaunchContext.getAccountFrontEndQuery();
        FrontEndQuery contactFrontEndQuery = playLaunchContext.getContactFrontEndQuery();
        List<Object> modifiableAccountIdCollectionForContacts = playLaunchContext
                .getModifiableAccountIdCollectionForContacts();
        String ratingId = playLaunchContext.getRatingId();
        if (playLaunchContext.getSegment() != null) {
            FrontEndRestriction accountRestriction = new FrontEndRestriction(
                    playLaunchContext.getSegment().getAccountRestriction());
            if (ratingId != null) {
                accountRestriction = createAccountRestrictionForSelectedRatings(playLaunchContext, ratingId);
            }

            FrontEndRestriction contactRestriction = new FrontEndRestriction(
                    playLaunchContext.getSegment().getContactRestriction());

            accountFrontEndQuery.setAccountRestriction(accountRestriction);
            accountFrontEndQuery.setContactRestriction(contactRestriction);

            Restriction extractedContactRestriction = contactRestriction.getRestriction() == null
                    ? LogicalRestriction.builder().or(new ArrayList<>()).build()
                    : contactRestriction.getRestriction();
            if (!useSpark) {
                contactFrontEndQuery.setContactRestriction(prepareContactRestriction(extractedContactRestriction,
                        modifiableAccountIdCollectionForContacts));
            } else {
                contactFrontEndQuery.setContactRestriction(new FrontEndRestriction(extractedContactRestriction));
            }
        }

        List<RatingModel> ratingModels = Collections.singletonList(playLaunchContext.getPublishedIteration());
        if (ratingId != null && CollectionUtils.isNotEmpty(ratingModels)) {
            accountFrontEndQuery.setRatingModels(ratingModels);

            // TODO add filtering based on list of selected buckets and update
            // selected vs skipped count accordingly
            List<Lookup> lookups = accountFrontEndQuery.getLookups();
            Lookup lookup = new AttributeLookup(BusinessEntity.Rating, ratingId);
            lookups.add(lookup);
            if (playLaunchContext.getRatingEngine().getType() != RatingEngineType.RULE_BASED) {
                lookups.add(new AttributeLookup(BusinessEntity.Rating, ratingId + "_score"));
            }
            if (playLaunchContext.getRatingEngine().getType() == RatingEngineType.CROSS_SELL
                    && ((AIModel) playLaunchContext.getPublishedIteration())
                            .getPredictionType() == PredictionType.EXPECTED_VALUE) {
                lookups.add(new AttributeLookup(BusinessEntity.Rating, ratingId + "_ev"));
            }
        }
    }

    private FrontEndRestriction createAccountRestrictionForSelectedRatings(PlayLaunchContext playLaunchContext,
            String ratingId) {
        Lookup lhs = new AttributeLookup(BusinessEntity.Rating, ratingId);

        Restriction ratingRestriction;
        if (CollectionUtils.isEmpty(playLaunchContext.getPlayLaunch().getBucketsToLaunch())) {
            ratingRestriction = null;
        } else {
            Collection<Object> allowedRatingsCollection = playLaunchContext.getPlayLaunch().getBucketsToLaunch()
                    .stream().map(RatingBucketName::getName).collect(Collectors.toList());
            Lookup rhs = new CollectionLookup(allowedRatingsCollection);
            ratingRestriction = new ConcreteRestriction(false, lhs, ComparisonType.IN_COLLECTION, rhs);
        }

        Restriction baseRestriction = playLaunchContext.getSegment().getAccountRestriction();

        ratingRestriction = playLaunchContext.getPlayLaunch().isLaunchUnscored() //
                ? Restriction.builder()
                        .or(ratingRestriction, new ConcreteRestriction(false, lhs, ComparisonType.IS_NULL, null))
                        .build()
                : ratingRestriction;

        Restriction finalAccountRestriction = Restriction.builder().and(baseRestriction, ratingRestriction).build();
        return new FrontEndRestriction(finalAccountRestriction);
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
        accountLookupFields.put(BusinessEntity.Account, Arrays.asList(InterfaceName.AccountId.name(), //
                InterfaceName.CompanyName.name(), //
                InterfaceName.LDC_Name.name()));

        contactLookupFields = new HashMap<>();
        contactLookupFields.put(BusinessEntity.Contact, Arrays.asList(InterfaceName.AccountId.name(), //
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
