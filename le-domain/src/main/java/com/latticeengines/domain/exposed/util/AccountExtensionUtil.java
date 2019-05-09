package com.latticeengines.domain.exposed.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.RestrictionBuilder;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.domain.exposed.security.Tenant;

public class AccountExtensionUtil {

    private static final Logger log = LoggerFactory.getLogger(AccountExtensionUtil.class);

    private static List<String> LOOKUP_FIELDS = Collections.singletonList(InterfaceName.AccountId.name());

    public static FrontEndQuery constructFrontEndQuery(String customerSpace, List<String> accountIds,
            String lookupIdColumn, Long start, boolean shouldAddLookupIdClause) {

        ArrayList<String> attributes = new ArrayList<String>(Arrays.asList(InterfaceName.AccountId.name()));
        return constructFrontEndQuery(customerSpace, accountIds, lookupIdColumn, attributes, start,
                shouldAddLookupIdClause);
    }

    public static FrontEndQuery constructFrontEndQuery(String customerSpace, List<String> accountIds,
            String lookupIdColumn, List<String> attributes, Long start, boolean shouldAddLookupIdClause) {

        List<Restriction> restrictions = new ArrayList<>();
        List<Restriction> idRestrictions = new ArrayList<>();

        if (start != null) {
            long lastModifiedTime = start;
            Restriction lastModifiedRestriction = Restriction.builder()
                    .let(BusinessEntity.Account, InterfaceName.CDLUpdatedTime.name()).gte(lastModifiedTime).build();
            restrictions.add(lastModifiedRestriction);
        }

        if (CollectionUtils.isNotEmpty(accountIds)) {
            RestrictionBuilder accoundIdRestrictionBuilder = Restriction.builder();
            RestrictionBuilder[] accountIdRestrictions = accountIds.stream()
                    .map(id -> Restriction.builder().let(BusinessEntity.Account, InterfaceName.AccountId.name()).eq(id))
                    .toArray(RestrictionBuilder[]::new);
            accoundIdRestrictionBuilder.or(accountIdRestrictions);
            idRestrictions.add(accoundIdRestrictionBuilder.build());

            if (shouldAddLookupIdClause && StringUtils.isNotBlank(lookupIdColumn)) {
                RestrictionBuilder sfdcRestrictionBuilder = Restriction.builder();
                RestrictionBuilder[] sfdcRestrictions = accountIds.stream()
                        .map(id -> Restriction.builder().let(BusinessEntity.Account, lookupIdColumn).eq(id))
                        .toArray(RestrictionBuilder[]::new);
                sfdcRestrictionBuilder.or(sfdcRestrictions);
                idRestrictions.add(sfdcRestrictionBuilder.build());
            }
        }

        if (CollectionUtils.isNotEmpty(idRestrictions)) {
            restrictions.add(Restriction.builder().or(idRestrictions).build());
        }

        Restriction restriction = Restriction.builder() //
                .and(restrictions) //
                .build();

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setAccountRestriction(new FrontEndRestriction(restriction));
        List<AttributeLookup> sortLookups = new ArrayList<>();
        sortLookups.add(new AttributeLookup(BusinessEntity.Account, InterfaceName.CDLUpdatedTime.name()));
        sortLookups.add(new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name()));
        FrontEndSort sort = new FrontEndSort(sortLookups, false);
        frontEndQuery.setSort(sort);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.addLookups(BusinessEntity.Account, attributes.toArray(new String[attributes.size()]));

        return frontEndQuery;
    }

    public static List<String> extractAccountIds(DataPage dataPage) {
        List<String> internalAccountIds = new ArrayList<String>();

        if (dataPage != null && CollectionUtils.isNotEmpty(dataPage.getData())) {
            internalAccountIds = dataPage.getData().stream()
                    .filter(internalAccoundIdObj -> internalAccoundIdObj.get(InterfaceName.AccountId.name()) != null)
                    .map(internalAccoundIdObj -> internalAccoundIdObj.get(InterfaceName.AccountId.name()).toString())
                    .collect(Collectors.toList());
        }

        log.info(String.format("Internal account ids to query: %s", internalAccountIds));
        return internalAccountIds;
    }

    public static MatchInput constructMatchInput(String customerSpace, List<String> internalAccountIds,
            Set<String> attributes, String dataCloudVersion) {

        MatchInput matchInput = new MatchInput();
        List<List<Object>> data = new ArrayList<>();
        internalAccountIds.forEach(accountId -> data.add(Collections.singletonList(accountId)));

        Tenant tenant = new Tenant(customerSpace);
        matchInput.setTenant(tenant);
        matchInput.setFields(LOOKUP_FIELDS);
        matchInput.setData(data);
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.LookupId, LOOKUP_FIELDS);
        matchInput.setKeyMap(keyMap);
        matchInput.setDataCloudVersion(dataCloudVersion);
        matchInput.setUseRemoteDnB(false);
        List<Column> columnSelections = attributes.stream().map(Column::new).collect(Collectors.toList());
        ColumnSelection columnSelection = new ColumnSelection();
        columnSelection.setColumns(columnSelections);
        matchInput.setCustomSelection(columnSelection);

        return matchInput;
    }

    public static MatchInput constructMatchInput(String customerSpace, List<String> internalAccountIds,
            ColumnSelection.Predefined predefined, String dataCloudVersion) {

        List<List<Object>> data = new ArrayList<>();
        internalAccountIds.forEach(accountId -> data.add(Collections.singletonList(accountId)));

        Tenant tenant = new Tenant(customerSpace);
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(tenant);
        matchInput.setFields(LOOKUP_FIELDS);
        matchInput.setData(data);
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.LookupId, LOOKUP_FIELDS);
        matchInput.setKeyMap(keyMap);
        matchInput.setPredefinedSelection(predefined);
        matchInput.setUseRemoteDnB(false);
        matchInput.setDataCloudVersion(dataCloudVersion);

        return matchInput;
    }

    /*
     * Reformats date attributes and converts matchOutput to data page
     */
    public static DataPage processMatchOutputResults(List<ColumnMetadata> dateAttributesMetadata,
            MatchOutput matchOutput) {
        DataPage dataPage = createEmptyDataPage();
        Map<String, ColumnMetadata> dateAttributesMap = dateAttributesMetadata.stream()
                .collect(Collectors.toMap(ColumnMetadata::getAttrName, cm -> cm, (cm1, cm2) -> {
                    log.info("duplicate key found! " + JsonUtils.serialize(cm1) + "/n" + JsonUtils.serialize(cm2));
                    return cm1;
                }));
        List<String> fields = matchOutput.getOutputFields();
        IntStream.range(0, matchOutput.getResult().size()) //
                .forEach(i -> {
                    Map<String, Object> data = null;
                    if (matchOutput != null //
                            && CollectionUtils.isNotEmpty(matchOutput.getResult()) //
                            && matchOutput.getResult().get(i) != null) {

                        if (matchOutput.getResult().get(i).isMatched() != Boolean.TRUE) {
                            log.info("No match on MatchApi, reverting to ObjectApi");
                        } else {
                            log.info("Found full match from lattice data cloud as well as from my data table.");

                            final Map<String, Object> tempDataRef = new HashMap<>();
                            List<Object> values = matchOutput.getResult().get(i).getOutput();
                            IntStream.range(0, fields.size()) //
                                    .forEach(j -> {
                                        Object value = values.get(j);
                                        if (dateAttributesMap.containsKey(fields.get(j))) {
                                            ColumnMetadata cm = dateAttributesMap.get(fields.get(j));
                                            log.info("Date attribute to reformat: " + JsonUtils.serialize(cm));
                                            final String DATE_FORMAT = "MM/dd/yyyy hh:mm:ss a z";
                                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATE_FORMAT);

                                            simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

                                            try {
                                                value = simpleDateFormat.format(value);
                                            } catch (Exception e) {
                                                log.info(String.format("Could not reformat date value %s for column %s",
                                                        String.valueOf(value), fields.get(j)));
                                            }
                                        }
                                        tempDataRef.put(fields.get(j), value);
                                    });
                            data = tempDataRef;
                        }

                    }

                    if (MapUtils.isNotEmpty(data)) {
                        dataPage.getData().add(data);
                    }
                });
        return dataPage;
    }

    public static DataPage convertToDataPage(MatchOutput matchOutput) {
        DataPage dataPage = createEmptyDataPage();
        List<String> fields = matchOutput.getOutputFields();
        IntStream.range(0, matchOutput.getResult().size()) //
                .forEach(i -> {
                    Map<String, Object> data = null;
                    if (matchOutput != null //
                            && CollectionUtils.isNotEmpty(matchOutput.getResult()) //
                            && matchOutput.getResult().get(i) != null) {

                        if (matchOutput.getResult().get(i).isMatched() != Boolean.TRUE) {
                            log.info("Didn't find any match from lattice data cloud. "
                                    + "Still continue to process the result as we may "
                                    + "have found partial match in my data table.");
                        } else {
                            log.info("Found full match from lattice data cloud as well as from my data table.");
                        }

                        final Map<String, Object> tempDataRef = new HashMap<>();
                        List<Object> values = matchOutput.getResult().get(i).getOutput();
                        IntStream.range(0, fields.size()) //
                                .forEach(j -> {
                                    tempDataRef.put(fields.get(j), values.get(j));
                                });
                        data = tempDataRef;

                    }

                    if (MapUtils.isNotEmpty(data)) {
                        dataPage.getData().add(data);
                    }
                });
        return dataPage;
    }

    public static DataPage createEmptyDataPage() {
        DataPage dataPage = new DataPage();
        List<Map<String, Object>> dataList = new ArrayList<>();
        dataPage.setData(dataList);
        return dataPage;
    }

}
