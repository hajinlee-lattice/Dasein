package com.latticeengines.ulysses.utils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

@Component(AccountDanteFormatter.Qualifier)
public class AccountDanteFormatter implements DanteFormatter<Map<String, Object>> {

    public static final String Qualifier = "accountDanteFormatter";

    private static final String notionName = "DanteAccount";
    private static final String defaultIsSegmentValue = Boolean.toString(false);
    private static final String accountIdColumnName = "AccountId";
    private static final String lookupIdColumnName = "SalesforceAccountID";
    private static final String isSegmentColumnName = "IsSegment";

    private static class RequiredDanteAccountProperty {
        public static String BaseExternalID = "BaseExternalID";
        public static String LEAccount_External_ID = "LEAccount_External_ID";
        public static String Dante_Accounts = "Dante_Accounts";
        public static String SalesforceAccountID = "SalesforceAccountID";
        public static String NotionName = "NotionName";
    }

    private static class DanteAccountSegmentProperty {
        public static String RepresentativeAccounts = "RepresentativeAccounts";
        public static String IsSegment = "IsSegment";
        public static String Segment1Name = "Segment1Name";
        public static String Segment2Name = "Segment2Name";
        public static String Segment3Name = "Segment3Name";
    }

    @Override
    @SuppressWarnings("unchecked")
    public String format(Map<String, Object> entity) {
        if (!entity.containsKey(accountIdColumnName) && !entity.containsKey(accountIdColumnName.toLowerCase())) {
            throw new LedpException(LedpCode.LEDP_39004);
        }

        boolean isSegment = isSpendAnalyticsSegmentEntity(entity);

        String accountId = (String) (isSegment ? entity.get(accountIdColumnName.toLowerCase()) : entity.get(accountIdColumnName));
        entity.put(RequiredDanteAccountProperty.BaseExternalID, accountId);
        entity.put(RequiredDanteAccountProperty.LEAccount_External_ID, accountId);
        entity.put(RequiredDanteAccountProperty.Dante_Accounts, accountId);
        entity.put(RequiredDanteAccountProperty.SalesforceAccountID,
                entity.containsKey(lookupIdColumnName) ? entity.get(lookupIdColumnName) : accountId);
        entity.put(RequiredDanteAccountProperty.NotionName, notionName);
        entity.put(DanteAccountSegmentProperty.IsSegment, isSegment);
        entity.put(DanteAccountSegmentProperty.Segment1Name, isSegment ? accountId : "");
        entity.put(DanteAccountSegmentProperty.Segment2Name, "");
        entity.put(DanteAccountSegmentProperty.Segment3Name, "");
        if (entity.containsKey(InterfaceName.RepresentativeAccounts.toString().toLowerCase())) {
            entity.put(InterfaceName.RepresentativeAccounts.toString(),
                    entity.get(InterfaceName.RepresentativeAccounts.toString().toLowerCase()));
        }
        if (isSegment) {
            entity.remove(accountIdColumnName.toLowerCase());
            entity.remove("spendanalyticssegment");
            entity.remove(InterfaceName.RepresentativeAccounts.toString().toLowerCase());
            entity.remove(DanteAccountSegmentProperty.IsSegment.toLowerCase());
        }

        return JsonUtils.serialize(entity);
    }

    private boolean isSpendAnalyticsSegmentEntity(Map<String, Object> entity) {
        return (entity.containsKey(isSegmentColumnName) //
                && entity.get(isSegmentColumnName) != null //
                && (boolean) entity.get(isSegmentColumnName)) //
                || (entity.containsKey(isSegmentColumnName.toLowerCase()) //
                && entity.get(isSegmentColumnName.toLowerCase()) != null //
                && (boolean) entity.get(isSegmentColumnName.toLowerCase()));
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<String> format(List<Map<String, Object>> entities) {
        return entities != null //
                ? entities.stream().map(this::format).collect(Collectors.toList()) //
                : Collections.EMPTY_LIST;
    }
}
