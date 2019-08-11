package com.latticeengines.domain.exposed.playmakercore;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public enum RecommendationColumName {
    PID, //
    EXTERNAL_ID, //
    ACCOUNT_ID, //
    LE_ACCOUNT_EXTERNAL_ID, //
    PLAY_ID, //
    LAUNCH_ID, //
    DESCRIPTION, //
    LAUNCH_DATE, //
    LAST_UPDATED_TIMESTAMP, //
    MONETARY_VALUE, //
    LIKELIHOOD, //
    COMPANY_NAME, //
    SFDC_ACCOUNT_ID, //
    PRIORITY_ID, //
    PRIORITY_DISPLAY_NAME, //
    MONETARY_VALUE_ISO4217_ID, //
    LIFT, //
    RATING_MODEL_ID, //
    MODEL_SUMMARY_ID, //
    SYNC_DESTINATION, //
    DESTINATION_ORG_ID, //
    DESTINATION_SYS_TYPE, //
    TENANT_ID, //
    DELETED;

    // this maps from Recommendation table column names to the internal name
    // provided by the export config service
    public static final Map<String, String> RECOMMENDATION_COLUMN_TO_INTERNAL_NAME_MAP = //
            ImmutableMap.<String, String> builder().put(PID.name(), PID.name()) //
                    .put(EXTERNAL_ID.name(), EXTERNAL_ID.name())//
                    .put(ACCOUNT_ID.name(), InterfaceName.AccountId.name()) //
                    .put(LE_ACCOUNT_EXTERNAL_ID.name(), LE_ACCOUNT_EXTERNAL_ID.name()) //
                    .put(PLAY_ID.name(), PLAY_ID.name()) //
                    .put(LAUNCH_ID.name(), LAUNCH_ID.name()) //
                    .put(DESCRIPTION.name(), DESCRIPTION.name()) //
                    .put(LAUNCH_DATE.name(), LAUNCH_DATE.name()) //
                    .put(LAST_UPDATED_TIMESTAMP.name(), LAST_UPDATED_TIMESTAMP.name()) //
                    .put(MONETARY_VALUE.name(), MONETARY_VALUE.name()) //
                    .put(LIKELIHOOD.name(), LIKELIHOOD.name()) //
                    .put(COMPANY_NAME.name(), InterfaceName.CompanyName.name()) //
                    .put(SFDC_ACCOUNT_ID.name(), InterfaceName.SalesforceAccountID.name()) //
                    .put(PRIORITY_ID.name(), PRIORITY_ID.name()) //
                    .put(PRIORITY_DISPLAY_NAME.name(), PRIORITY_DISPLAY_NAME.name()) //
                    .put(MONETARY_VALUE_ISO4217_ID.name(), MONETARY_VALUE_ISO4217_ID.name()) //
                    .put(LIFT.name(), LIFT.name()) //
                    .put(RATING_MODEL_ID.name(), RATING_MODEL_ID.name()) //
                    .put(MODEL_SUMMARY_ID.name(), MODEL_SUMMARY_ID.name()) //
                    .put(SYNC_DESTINATION.name(), SYNC_DESTINATION.name()) //
                    .put(DESTINATION_ORG_ID.name(), DESTINATION_ORG_ID.name()) //
                    .put(DESTINATION_SYS_TYPE.name(), DESTINATION_SYS_TYPE.name()) //
                    .put(TENANT_ID.name(), TENANT_ID.name()) //
                    .put(DELETED.name(), DELETED.name()) //
                    .build();

    // this maps from internal name to Recommendation table column
    public static final Map<String, String> INTERNAL_NAME_TO_RECOMMENDATION_COLUMN_MAP = //
            ImmutableMap.<String, String> builder().put(PID.name(), PID.name()) //
                    .put(EXTERNAL_ID.name(), EXTERNAL_ID.name())//
                    .put(InterfaceName.AccountId.name(), ACCOUNT_ID.name()) //
                    .put(LE_ACCOUNT_EXTERNAL_ID.name(), LE_ACCOUNT_EXTERNAL_ID.name()) //
                    .put(PLAY_ID.name(), PLAY_ID.name()) //
                    .put(LAUNCH_ID.name(), LAUNCH_ID.name()) //
                    .put(DESCRIPTION.name(), DESCRIPTION.name()) //
                    .put(LAUNCH_DATE.name(), LAUNCH_DATE.name()) //
                    .put(LAST_UPDATED_TIMESTAMP.name(), LAST_UPDATED_TIMESTAMP.name()) //
                    .put(MONETARY_VALUE.name(), MONETARY_VALUE.name()) //
                    .put(LIKELIHOOD.name(), LIKELIHOOD.name()) //
                    .put(InterfaceName.CompanyName.name(), COMPANY_NAME.name()) //
                    .put(InterfaceName.SalesforceAccountID.name(), SFDC_ACCOUNT_ID.name()) //
                    .put(PRIORITY_ID.name(), PRIORITY_ID.name()) //
                    .put(PRIORITY_DISPLAY_NAME.name(), PRIORITY_DISPLAY_NAME.name()) //
                    .put(MONETARY_VALUE_ISO4217_ID.name(), MONETARY_VALUE_ISO4217_ID.name()) //
                    .put(LIFT.name(), LIFT.name()) //
                    .put(RATING_MODEL_ID.name(), RATING_MODEL_ID.name()) //
                    .put(MODEL_SUMMARY_ID.name(), MODEL_SUMMARY_ID.name()) //
                    .put(SYNC_DESTINATION.name(), SYNC_DESTINATION.name()) //
                    .put(DESTINATION_ORG_ID.name(), DESTINATION_ORG_ID.name()) //
                    .put(DESTINATION_SYS_TYPE.name(), DESTINATION_SYS_TYPE.name()) //
                    .put(TENANT_ID.name(), TENANT_ID.name()) //
                    .put(DELETED.name(), DELETED.name()) //
                    .build();
}
