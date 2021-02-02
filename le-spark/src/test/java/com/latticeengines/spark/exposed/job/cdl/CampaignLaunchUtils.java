package com.latticeengines.spark.exposed.job.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;

import com.google.common.collect.ImmutableList;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.playmakercore.NonStandardRecColumnName;
import com.latticeengines.domain.exposed.playmakercore.RecommendationColumnName;

public final class CampaignLaunchUtils {

    public static final String SFDC_ACCOUNT_ID = "SFDC_ACCOUNT_ID";
    public static final String SFDC_CONTACT_ID = "SFDC_CONTACT_ID";

    protected CampaignLaunchUtils() {
        throw new UnsupportedOperationException();
    }
    // ---- start of Facebook Account and Contact columns---
    public static List<String> facebookRecommendationAccountColumns() {
        return ImmutableList.<String> builder().addAll(generateAccountColsRecIncludedForFacebook())
                .addAll(generateAccountColsRecNotIncludedStdForFacebook())
                .addAll(generateAccountColsRecNotIncludedNonStdForFacebook())
                .addAll(CollectionUtils.isNotEmpty(generateContactColsForFacebook())
                        ? Collections.singletonList(RecommendationColumnName.CONTACTS.name())
                        : Collections.emptyList())
                .build();
    }

    public static List<String> facebookRecommendationContactColumns() {
        return generateContactColsForFacebook();
    }

    public static List<String> generateAccountColsRecIncludedForFacebook() {
        return Arrays.asList(InterfaceName.CompanyName.name());
    }

    public static List<String> generateAccountColsRecNotIncludedStdForFacebook() {
        return Arrays.asList(InterfaceName.Website.name());
    }

    public static List<String> generateAccountColsRecNotIncludedNonStdForFacebook() {
        return Collections.emptyList();
    }

    public static List<String> generateContactColsForFacebook() {
        return Arrays.asList(InterfaceName.FirstName.name(), InterfaceName.LastName.name(), InterfaceName.Email.name(),
                InterfaceName.Country.name(), InterfaceName.PhoneNumber.name(), InterfaceName.PostalCode.name(),
                InterfaceName.City.name(), InterfaceName.ContactId.name(), InterfaceName.State.name());
    }

    // ---- end of Facebook Account and Contact columns---

    // ---- start of Google Account and Contact columns---
    public static List<String> googleRecommendationAccountColumns() {
        return ImmutableList.<String> builder().addAll(generateAccountColsRecIncludedForGoogle())
                .addAll(generateAccountColsRecNotIncludedStdForGoogle())
                .addAll(generateAccountColsRecNotIncludedNonStdForGoogle())
                .addAll(CollectionUtils.isNotEmpty(generateContactColsForGoogle())
                        ? Collections.singletonList(RecommendationColumnName.CONTACTS.name())
                        : Collections.emptyList())
                .build();
    }

    public static List<String> googleRecommendationContactColumns() {
        return generateContactColsForGoogle();
    }

    public static List<String> generateAccountColsRecIncludedForGoogle() {
        return Collections.emptyList();
    }

    public static List<String> generateAccountColsRecNotIncludedStdForGoogle() {
        return Collections.emptyList();
    }

    public static List<String> generateAccountColsRecNotIncludedNonStdForGoogle() {
        return Collections.emptyList();
    }

    public static List<String> generateContactColsForGoogle() {
        return Arrays.asList(InterfaceName.FirstName.name(), InterfaceName.LastName.name(), InterfaceName.Email.name(),
                InterfaceName.Country.name(), InterfaceName.PhoneNumber.name(), InterfaceName.PostalCode.name());
    }

    // ---- end of Google Account and Contact columns---

    // ---- start of LinkedIn Account and Contact columns---
    public static List<String> linkedInRecommendationAccountColumns() {
        return ImmutableList.<String> builder().addAll(generateAccountColsRecIncludedForLinkedIn())
                .addAll(generateAccountColsRecNotIncludedStdForLinkedIn())
                .addAll(generateAccountColsRecNotIncludedNonStdForLinkedIn())
                .addAll(CollectionUtils.isNotEmpty(generateContactColsForLinkedIn())
                        ? Collections.singletonList(RecommendationColumnName.CONTACTS.name())
                        : Collections.emptyList())
                .build();
    }

    public static List<String> linkedInRecommendationContactColumns() {
        return generateContactColsForLinkedIn();
    }

    public static List<String> generateAccountColsRecIncludedForLinkedIn() {
        return Arrays.asList(RecommendationColumnName.COMPANY_NAME.name()).stream()
                .map(col -> RecommendationColumnName.RECOMMENDATION_COLUMN_TO_INTERNAL_NAME_MAP.getOrDefault(col, col))
                .collect(Collectors.toList());
    }

    public static List<String> generateAccountColsRecNotIncludedStdForLinkedIn() {
        return Arrays.asList(InterfaceName.Website.name());
    }

    public static List<String> generateAccountColsRecNotIncludedNonStdForLinkedIn() {
        return Collections.emptyList();
    }

    public static List<String> generateContactColsForLinkedIn() {
        return Arrays.asList(InterfaceName.Email.name());
    }

    // ---- end of LinkedIn Account and Contact columns---

    // ---- start of S3 Account and Contact columns---

    public static List<String> s3RecommendationAccountColumns() {
        return ImmutableList.<String> builder().addAll(generateAccountColsRecIncludedForS3())
                .addAll(generateAccountColsRecNotIncludedStdForS3())
                .addAll(generateAccountColsRecNotIncludedNonStdForS3())
                .addAll(CollectionUtils.isNotEmpty(generateContactColsForS3())
                        ? Collections.singletonList(RecommendationColumnName.CONTACTS.name())
                        : Collections.emptyList())
                .build();
    }

    public static List<String> s3RecommendationContactColumns() {
        return generateContactColsForS3();
    }

    public static List<String> generateRecommendationOutputAccountCols() {
        return new ArrayList<String>(
                Arrays.asList(RecommendationColumnName.COMPANY_NAME.name(), InterfaceName.AccountId.name(),
                RecommendationColumnName.DESCRIPTION.name(), RecommendationColumnName.PLAY_ID.name(),
                RecommendationColumnName.LAUNCH_DATE.name(), RecommendationColumnName.LAUNCH_ID.name(),
                RecommendationColumnName.DESTINATION_ORG_ID.name(),
                RecommendationColumnName.DESTINATION_SYS_TYPE.name(),
                RecommendationColumnName.LAST_UPDATED_TIMESTAMP.name(), RecommendationColumnName.MONETARY_VALUE.name(),
                RecommendationColumnName.PRIORITY_ID.name(), RecommendationColumnName.PRIORITY_DISPLAY_NAME.name(),
                RecommendationColumnName.LIKELIHOOD.name(), RecommendationColumnName.LIFT.name(),
                        RecommendationColumnName.RATING_MODEL_ID.name(), RecommendationColumnName.EXTERNAL_ID.name()));
    }

    public static List<String> generateAccountColsRecIncludedForS3() {
        return generateRecommendationOutputAccountCols()
                .stream()
                .map(col -> RecommendationColumnName.RECOMMENDATION_COLUMN_TO_INTERNAL_NAME_MAP.getOrDefault(col, col))
                .collect(Collectors.toList());
    }

    public static List<String> generateAccountColsRecNotIncludedStdForS3() {
        return Arrays.asList(InterfaceName.Website.name(), InterfaceName.CreatedDate.name());
    }

    public static List<String> generateAccountColsRecNotIncludedNonStdForS3() {
        return Arrays.asList(NonStandardRecColumnName.DESTINATION_SYS_NAME.name(),
                NonStandardRecColumnName.PLAY_NAME.name(), NonStandardRecColumnName.RATING_MODEL_NAME.name(),
                NonStandardRecColumnName.SEGMENT_NAME.name());
    }

    public static List<String> generateContactColsForS3() {
        return new ArrayList<String>(Arrays.asList(InterfaceName.Email.name(), InterfaceName.Address_Street_1.name(),
                InterfaceName.PhoneNumber.name(), InterfaceName.State.name(), InterfaceName.PostalCode.name(),
                InterfaceName.Country.name(), InterfaceName.SalesforceContactID.name(), InterfaceName.City.name(),
                InterfaceName.ContactId.name(), InterfaceName.Name.name(), InterfaceName.FirstName.name(),
                InterfaceName.LastName.name()));
    }

    // ---- end of S3 Account and Contact columns---

    // ---- start of Marketo Account and Contact columns---

    public static List<String> marketoRecommendationAccountColumns() {
        return ImmutableList.<String> builder().addAll(generateAccountColsRecIncludedForMarketo())
                .addAll(generateAccountColsRecNotIncludedStdForMarketo())
                .addAll(generateAccountColsRecNotIncludedNonStdForMarketo())
                .addAll(CollectionUtils.isNotEmpty(generateContactColsForMarketo())
                        ? Collections.singletonList(RecommendationColumnName.CONTACTS.name())
                        : Collections.emptyList())
                .build();
    }

    public static List<String> marketoRecommendationContactColumns() {
        return generateContactColsForMarketo();
    }

    public static List<String> generateAccountColsRecIncludedForMarketo() {
        return Arrays
                .asList(RecommendationColumnName.ACCOUNT_ID.name(), RecommendationColumnName.PLAY_ID.name(),
                        RecommendationColumnName.MONETARY_VALUE.name(), RecommendationColumnName.LIKELIHOOD.name(),
                        RecommendationColumnName.COMPANY_NAME.name(), RecommendationColumnName.PRIORITY_ID.name(),
                        RecommendationColumnName.PRIORITY_DISPLAY_NAME.name(), RecommendationColumnName.LIFT.name(),
                        RecommendationColumnName.RATING_MODEL_ID.name())
                .stream()
                .map(col -> RecommendationColumnName.RECOMMENDATION_COLUMN_TO_INTERNAL_NAME_MAP.getOrDefault(col, col))
                .collect(Collectors.toList());
    }

    public static List<String> generateAccountColsRecNotIncludedStdForMarketo() {
        return Arrays.asList(InterfaceName.Website.name());
    }

    public static List<String> generateAccountColsRecNotIncludedNonStdForMarketo() {
        return Arrays.asList(NonStandardRecColumnName.DESTINATION_SYS_NAME.name(),
                NonStandardRecColumnName.PLAY_NAME.name(), NonStandardRecColumnName.RATING_MODEL_NAME.name(),
                NonStandardRecColumnName.SEGMENT_NAME.name());
    }

    public static List<String> generateContactColsForMarketo() {
        return Arrays.asList(InterfaceName.FirstName.name(), InterfaceName.LastName.name(), InterfaceName.Email.name(),
                InterfaceName.Country.name(), InterfaceName.PhoneNumber.name(), InterfaceName.PostalCode.name(),
                InterfaceName.Address_Street_1.name(), InterfaceName.City.name(), InterfaceName.ContactName.name(),
                InterfaceName.State.name());
    }

    // ---- end of Marketo Account and Contact columns---

    public static String generateExpectedSfdcAccountIdCol(String sfdcAccountId, boolean isEntityMatch) {
        if (sfdcAccountId != null) {
            return sfdcAccountId;
        }
        if (isEntityMatch) {
            return InterfaceName.CustomerAccountId.name();
        }
        return SFDC_ACCOUNT_ID;
    }

    public static String generateExpectedSfdcContactIdCol(String sfdcContactId, boolean isEntityMatch) {
        if (sfdcContactId != null) {
            return sfdcContactId;
        }
        if (isEntityMatch) {
            return InterfaceName.CustomerContactId.name();
        }
        return SFDC_CONTACT_ID;
    }
}
