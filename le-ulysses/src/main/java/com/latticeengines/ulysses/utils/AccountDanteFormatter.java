package com.latticeengines.ulysses.utils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.web.context.WebApplicationContext;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

@Component(AccountDanteFormatter.Qualifier)
@Scope(WebApplicationContext.SCOPE_REQUEST)
public class AccountDanteFormatter implements DanteFormatter<Map<String, Object>> {

    public static final String Qualifier = "accountDanteFormatter";

    private static final String notionName = "DanteAccount";
    @SuppressWarnings("unused")
    private static final String defaultIsSegmentValue = Boolean.toString(false);
    private static final String accountIdColumnName = InterfaceName.AccountId.name();
    private static final String custAccountIdColumnName = InterfaceName.CustomerAccountId.name();
    private static final String lookupIdColumnName = InterfaceName.SalesforceAccountID.name();
    private static final String isSegmentColumnName = "IsSegment";
    private boolean isEntityMatchEnabled = false;

    private static class RequiredDanteAccountProperty {
        public static String BaseExternalID = "BaseExternalID";
        public static String LEAccount_External_ID = "LEAccount_External_ID";
        public static String Dante_Accounts = "Dante_Accounts";
        public static String SalesforceAccountID = InterfaceName.SalesforceAccountID.name();
        public static String NotionName = "NotionName";
    }

    private static class DanteAccountSegmentProperty {
        @SuppressWarnings("unused")
        public static String RepresentativeAccounts = "RepresentativeAccounts";
        public static String IsSegment = "IsSegment";
        public static String Segment1Name = "Segment1Name";
        public static String Segment2Name = "Segment2Name";
        public static String Segment3Name = "Segment3Name";
        public static String DisplayName = "DisplayName";
    }

    public void setIsEntityMatchEnabled(boolean isEntityMatchEnabled) {
        this.isEntityMatchEnabled = isEntityMatchEnabled;
    }

    @Override
    public String format(Map<String, Object> entity) {
        if (!entity.containsKey(accountIdColumnName) && !entity.containsKey(accountIdColumnName.toLowerCase())) {
            throw new LedpException(LedpCode.LEDP_39004);
        }

        boolean isSegment = isSpendAnalyticsSegmentEntity(entity);

        String spendAnalyticsSegmentName = entity.containsKey(InterfaceName.SpendAnalyticsSegment.name())
                && entity.get(InterfaceName.SpendAnalyticsSegment.name()) != null
                        ? entity.get(InterfaceName.SpendAnalyticsSegment.name()).toString()
                        : null;

        String accountIdValue = (String) (isSegment ? entity.get(accountIdColumnName.toLowerCase())
                : entity.get(accountIdColumnName));
        accountIdValue = (String) (!isSegment && isEntityMatchEnabled ? entity.get(custAccountIdColumnName)
                : accountIdValue);
        entity.put(RequiredDanteAccountProperty.BaseExternalID, accountIdValue);
        entity.put(RequiredDanteAccountProperty.LEAccount_External_ID, accountIdValue);
        entity.put(RequiredDanteAccountProperty.Dante_Accounts, accountIdValue);
        entity.put(RequiredDanteAccountProperty.SalesforceAccountID,
                entity.getOrDefault(lookupIdColumnName, accountIdValue));
        entity.put(RequiredDanteAccountProperty.NotionName, notionName);
        entity.put(DanteAccountSegmentProperty.IsSegment, isSegment);
        entity.put(DanteAccountSegmentProperty.Segment1Name, isSegment ? accountIdValue : spendAnalyticsSegmentName);
        entity.put(DanteAccountSegmentProperty.Segment2Name, "");
        entity.put(DanteAccountSegmentProperty.Segment3Name, "");
        entity.put(DanteAccountSegmentProperty.DisplayName,
                isSegment ? accountIdValue : entity.get(InterfaceName.CompanyName.name().toLowerCase()));
        if (entity.containsKey(InterfaceName.RepresentativeAccounts.toString().toLowerCase())) {
            entity.put(InterfaceName.RepresentativeAccounts.toString(),
                    entity.get(InterfaceName.RepresentativeAccounts.toString().toLowerCase()));
        }
        if (isSegment) {
            entity.remove(accountIdColumnName.toLowerCase());
            entity.remove(InterfaceName.SpendAnalyticsSegment.name().toLowerCase());
            entity.remove(InterfaceName.RepresentativeAccounts.toString().toLowerCase());
            entity.remove(DanteAccountSegmentProperty.IsSegment.toLowerCase());
        }
        resetIsEntityMatchEnabled();
        return JsonUtils.serialize(entity);
    }

    private void resetIsEntityMatchEnabled() {
        this.setIsEntityMatchEnabled(false);
    }

    private boolean isSpendAnalyticsSegmentEntity(Map<String, Object> entity) {
        return (entity.containsKey(isSegmentColumnName) && entity.get(isSegmentColumnName) != null
                && (boolean) entity.get(isSegmentColumnName)) //
                || (entity.containsKey(isSegmentColumnName.toLowerCase())
                        && entity.get(isSegmentColumnName.toLowerCase()) != null
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
