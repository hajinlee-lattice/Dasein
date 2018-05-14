package com.latticeengines.ulysses.utils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component(AccountDanteFormatter.Qualifier)
public class AccountDanteFormatter implements DanteFormatter<Map<String, Object>> {

    public static final String Qualifier = "accountDanteFormatter";

    private static final String notionName = "DanteAccount";
    private static final String defaultIsSegmentValue = Boolean.toString(false);
    private static final String accountIdColumnName = "AccountId";
    private static final String lookupIdColumnName = "SalesforceAccountID";

    private static class RequiredDanteAccountProperty {
        public static String BaseExternalID = "BaseExternalID";
        public static String LEAccount_External_ID = "LEAccount_External_ID";
        public static String Dante_Accounts = "Dante_Accounts";
        public static String SalesforceAccountID = "SalesforceAccountID";
        public static String NotionName = "NotionName";
        public static String IsSegment = "IsSegment";
        public static String Segment1Name = "Segment1Name";
        public static String Segment2Name = "Segment2Name";
        public static String Segment3Name = "Segment3Name";
    }

    @Override
    @SuppressWarnings("unchecked")
    public String format(Map<String, Object> entity) {
        if (!entity.keySet().contains(accountIdColumnName)) {
            throw new LedpException(LedpCode.LEDP_39004);
        }
        String accountId = (String) entity.get(accountIdColumnName);
        entity.put(RequiredDanteAccountProperty.BaseExternalID, accountId);
        entity.put(RequiredDanteAccountProperty.LEAccount_External_ID, accountId);
        entity.put(RequiredDanteAccountProperty.Dante_Accounts, accountId);
        entity.put(RequiredDanteAccountProperty.SalesforceAccountID, entity.get(lookupIdColumnName));
        entity.put(RequiredDanteAccountProperty.NotionName, notionName);
        entity.put(RequiredDanteAccountProperty.IsSegment, defaultIsSegmentValue);
        entity.put(RequiredDanteAccountProperty.Segment1Name, "");
        entity.put(RequiredDanteAccountProperty.Segment2Name, "");
        entity.put(RequiredDanteAccountProperty.Segment3Name, "");

        return JsonUtils.serialize(entity);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<String> format(List<Map<String, Object>> entities) {
        return entities != null //
                ? entities.stream().map(this::format).collect(Collectors.toList()) //
                : Collections.EMPTY_LIST;
    }
}
