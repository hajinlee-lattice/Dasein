package com.latticeengines.apps.core.service.impl;

import java.util.Arrays;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.DataLicense;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrSpecification;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceapps.core.AttrSubType;
import com.latticeengines.domain.exposed.serviceapps.core.AttrType;

public class AttrConfigTestUtils {
    public static AttrConfig getLDCNonPremiumAttr(Category category, boolean enableThisAttr) {
        return getLDCNonPremiumAttr(category, enableThisAttr, false, false, false, false);
    }

    public static AttrConfig getLDCPremiumAttr(Category category, boolean enableThisAttr) {
        return getLDCPremiumAttr(category, enableThisAttr, false, false, false, false);
    }

    public static AttrConfig getLDCInternalAttr(Category category, boolean enableThisAttr) {
        return getLDCInternalAttr(category, enableThisAttr, false, false, false, false);
    }

    public static AttrConfig getCDLStdAttr(Category category, boolean enableThisAttr) {
        return getCDLStdAttr(category, enableThisAttr, false, false, false, false);
    }

    public static AttrConfig getCDLLookIDAttr(Category category, boolean enableThisAttr) {
        return getCDLLookIDAttr(category, enableThisAttr, false, false, false, false);
    }

    public static AttrConfig getCDLAccountExtensionAttr(Category category, boolean enableThisAttr) {
        return getCDLAccountExtensionAttr(category, enableThisAttr, false, false, false, false);
    }

    public static AttrConfig getCDLContactExtensionAttr(Category category, boolean enableThisAttr) {
        return getCDLContactExtensionAttr(category, enableThisAttr, false, false, false, false);
    }

    public static AttrConfig getCDLDerivedPBAttr(Category category, boolean enableThisAttr) {
        return getCDLDerivedPBAttr(category, enableThisAttr, false, false, false, false);
    }

    public static AttrConfig getCDLRatingAttr(Category category, boolean enableThisAttr) {
        return getCDLRatingAttr(category, enableThisAttr, false, false, false, false);
    }

    public static AttrConfig getLDCNonPremiumAttr(Category category, boolean enableThisAttr, boolean useForSegment,
            boolean useForEnrichment, boolean useForCompanyProfile, boolean useForTalkingPoint) {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("LDC Non-Premium");
        attrConfig.setAttrType(AttrType.DataCloud);
        attrConfig.setAttrSubType(AttrSubType.Normal);
        attrConfig.setEntity(BusinessEntity.Account);
        AttrSpecification attrSpec = AttrSpecification.getAttrSpecification(attrConfig.getAttrType(),
                attrConfig.getAttrSubType(), attrConfig.getEntity());

        AttrConfigProp<Boolean> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(attrSpec.allowChange(ColumnSelection.Predefined.Enrichment));
        prop1.setSystemValue(true);
        if (useForEnrichment) {
            prop1.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Enrichment.getName(), prop1);

        AttrConfigProp<Boolean> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(attrSpec.allowChange(ColumnSelection.Predefined.CompanyProfile));
        prop2.setSystemValue(true);
        if (useForCompanyProfile) {
            prop2.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.CompanyProfile.getName(), prop2);

        AttrConfigProp<Category> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(attrSpec.categoryNameChange());
        prop3.setSystemValue(category);
        attrConfig.putProperty(ColumnMetadataKey.Category, prop3);

        AttrConfigProp<String> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(attrSpec.descriptionChange());
        prop4.setSystemValue(
                "Changes during the last 30 days in users showing intent activity on Recruiting and Hiring");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(attrSpec.categoryNameChange());
        prop5.setSystemValue("Other");
        attrConfig.putProperty(ColumnMetadataKey.Subcategory, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(attrSpec.allowChange(ColumnSelection.Predefined.TalkingPoint));
        prop6.setSystemValue(true);
        if (useForTalkingPoint) {
            prop6.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);

        AttrConfigProp<AttrState> prop7 = new AttrConfigProp<>();
        prop7.setAllowCustomization(attrSpec.stateChange());
        prop7.setSystemValue(AttrState.Inactive);
        if (enableThisAttr) {
            prop7.setCustomValue(AttrState.Active);
        }
        attrConfig.putProperty(ColumnMetadataKey.State, prop7);

        AttrConfigProp<String> prop8 = new AttrConfigProp<>();
        prop8.setAllowCustomization(attrSpec.displayNameChange());
        prop8.setSystemValue("Recruiting & Hiring Intent Users Change (Deprecated)");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop8);

        AttrConfigProp<Boolean> prop9 = new AttrConfigProp<>();
        prop9.setAllowCustomization(attrSpec.allowChange(ColumnSelection.Predefined.Model));
        prop9.setSystemValue(false);
        attrConfig.putProperty(ColumnSelection.Predefined.Model.getName(), prop9);

        AttrConfigProp<Boolean> prop10 = new AttrConfigProp<>();
        prop10.setAllowCustomization(attrSpec.allowChange(ColumnSelection.Predefined.Segment));
        prop10.setSystemValue(false);
        if (useForSegment) {
            prop10.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop10);

        AttrConfigProp<String> prop11 = new AttrConfigProp<>();
        prop11.setAllowCustomization(attrSpec.approvedUsageChange());
        prop11.setSystemValue("[null]");
        attrConfig.putProperty(ColumnMetadataKey.ApprovedUsage, prop11);
        return attrConfig;
    }

    public static ColumnMetadata getLDCNonPremiumData(Category category) {
        ColumnMetadata data = new ColumnMetadata();
        data.setAttrName("LDC Non-Premium");
        data.setEntity(BusinessEntity.Account);

        data.setCategory(category);
        data.setDescription(
                "Changes during the last 30 days in users showing intent activity on Recruiting and Hiring");
        data.setSubcategory("Other");
        data.setAttrState(AttrState.Inactive);
        data.setDisplayName("Recruiting & Hiring Intent Users Change (Deprecated)");
        data.enableGroup(ColumnSelection.Predefined.Enrichment);
        data.enableGroup(ColumnSelection.Predefined.CompanyProfile);
        data.enableGroup(ColumnSelection.Predefined.TalkingPoint);
        data.setCanModel(true);
        data.setCanSegment(true);
        data.setCanEnrich(true);
        return data;
    }

    public static AttrConfig getLDCPremiumAttr(Category category, boolean enableThisAttr, boolean useForSegment,
            boolean useForEnrichment, boolean useForCompanyProfile, boolean useForTalkingPoint) {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("LDC Premium");
        attrConfig.setAttrType(AttrType.DataCloud);
        attrConfig.setAttrSubType(AttrSubType.Premium);
        attrConfig.setDataLicense(DataLicense.HG.toString());
        attrConfig.setEntity(BusinessEntity.Account);

        AttrSpecification attrSpec = AttrSpecification.getAttrSpecification(attrConfig.getAttrType(),
                attrConfig.getAttrSubType(), attrConfig.getEntity());
        AttrConfigProp<Boolean> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(attrSpec.enrichmentChange());
        prop1.setSystemValue(true);
        if (useForEnrichment) {
            prop1.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Enrichment.getName(), prop1);

        AttrConfigProp<Boolean> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(attrSpec.companyProfileChange());
        prop2.setSystemValue(true);
        if (useForCompanyProfile) {
            prop2.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.CompanyProfile.getName(), prop2);

        AttrConfigProp<Category> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(attrSpec.categoryNameChange());
        prop3.setSystemValue(category);
        attrConfig.putProperty(ColumnMetadataKey.Category, prop3);

        AttrConfigProp<String> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(attrSpec.descriptionChange());
        prop4.setSystemValue(
                "The Intent Score of the business shown in the topic - Permission Email Marketing. Normalized between from 0-100, with greater than 60 meaning a surge in the topic and lesser than 60 meaning average to low intent based on the score.");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(attrSpec.categoryNameChange());
        prop5.setSystemValue("Email Marketing");
        attrConfig.putProperty(ColumnMetadataKey.Subcategory, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(attrSpec.talkingPointChange());
        prop6.setSystemValue(true);
        if (useForTalkingPoint) {
            prop6.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);

        AttrConfigProp<AttrState> prop7 = new AttrConfigProp<>();
        prop7.setAllowCustomization(attrSpec.stateChange());
        prop7.setSystemValue(AttrState.Inactive);
        if (enableThisAttr) {
            prop7.setCustomValue(AttrState.Active);
        }
        attrConfig.putProperty(ColumnMetadataKey.State, prop7);

        AttrConfigProp<String> prop8 = new AttrConfigProp<>();
        prop8.setAllowCustomization(attrSpec.displayNameChange());
        prop8.setSystemValue("Permission Email Marketing Raw Score");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop8);

        AttrConfigProp<Boolean> prop9 = new AttrConfigProp<>();
        prop9.setAllowCustomization(attrSpec.modelChange());
        prop9.setSystemValue(false);
        attrConfig.putProperty(ColumnSelection.Predefined.Model.name(), prop9);

        AttrConfigProp<Boolean> prop10 = new AttrConfigProp<>();
        prop10.setAllowCustomization(attrSpec.segmentationChange());
        prop10.setSystemValue(false);
        if (useForSegment) {
            prop10.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop10);

        return attrConfig;
    }

    public static ColumnMetadata getLDCPremiumData(Category category) {
        ColumnMetadata data = new ColumnMetadata();
        data.setAttrName("LDC Premium");
        data.setEntity(BusinessEntity.Account);
        data.setDataLicense(DataLicense.HG.toString());

        data.setCategory(category);
        data.setDescription(
                "The Intent Score of the business shown in the topic - Permission Email Marketing. Normalized between from 0-100, with greater than 60 meaning a surge in the topic and lesser than 60 meaning average to low intent based on the score.");
        data.setSubcategory("Email Marketing");
        data.setAttrState(AttrState.Inactive);
        data.setDisplayName("Recruiting & Hiring Intent Users Change (Deprecated)");
        data.enableGroup(ColumnSelection.Predefined.Enrichment);
        data.enableGroup(ColumnSelection.Predefined.CompanyProfile);
        data.enableGroup(ColumnSelection.Predefined.TalkingPoint);
        return data;
    }

    public static AttrConfig getLDCInternalAttr(Category category, boolean enableThisAttr, boolean useForSegment,
            boolean useForEnrichment, boolean useForCompanyProfile, boolean useForTalkingPoint) {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("LDC Internal");
        attrConfig.setAttrType(AttrType.DataCloud);
        attrConfig.setAttrSubType(AttrSubType.InternalEnrich);
        attrConfig.setEntity(BusinessEntity.Account);

        AttrSpecification attrSpec = AttrSpecification.getAttrSpecification(attrConfig.getAttrType(),
                attrConfig.getAttrSubType(), attrConfig.getEntity());
        AttrConfigProp<Boolean> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(attrSpec.enrichmentChange());
        prop1.setSystemValue(true);
        if (useForEnrichment) {
            prop1.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Enrichment.getName(), prop1);

        AttrConfigProp<Boolean> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(attrSpec.companyProfileChange());
        prop2.setSystemValue(true);
        if (useForCompanyProfile) {
            prop2.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.CompanyProfile.getName(), prop2);

        AttrConfigProp<Category> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(attrSpec.categoryNameChange());
        prop3.setSystemValue(category);
        attrConfig.putProperty(ColumnMetadataKey.Category, prop3);

        AttrConfigProp<String> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(attrSpec.descriptionChange());
        prop4.setSystemValue(
                "The Intent Score of the business shown in the topic - Investor Relations. Normalized between from 0-100, with greater than 60 meaning a surge in the topic and lesser than 60 meaning average to low intent based on the score.");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(attrSpec.categoryNameChange());
        prop5.setSystemValue("Corporate Finance");
        attrConfig.putProperty(ColumnMetadataKey.Subcategory, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(attrSpec.talkingPointChange());
        prop6.setSystemValue(true);
        if (useForTalkingPoint) {
            prop6.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);

        AttrConfigProp<AttrState> prop7 = new AttrConfigProp<>();
        prop7.setAllowCustomization(attrSpec.stateChange());
        prop7.setSystemValue(AttrState.Inactive);
        if (enableThisAttr) {
            prop7.setCustomValue(AttrState.Active);
        }
        attrConfig.putProperty(ColumnMetadataKey.State, prop7);

        AttrConfigProp<String> prop8 = new AttrConfigProp<>();
        prop8.setAllowCustomization(attrSpec.displayNameChange());
        prop8.setSystemValue("Investor Relations Raw Score");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop8);

        AttrConfigProp<Boolean> prop9 = new AttrConfigProp<>();
        prop9.setAllowCustomization(attrSpec.modelChange());
        prop9.setSystemValue(false);
        attrConfig.putProperty(ColumnSelection.Predefined.Model.getName(), prop9);

        AttrConfigProp<Boolean> prop10 = new AttrConfigProp<>();
        prop10.setAllowCustomization(attrSpec.segmentationChange());
        prop10.setSystemValue(false);
        if (useForSegment) {
            prop10.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop10);

        return attrConfig;
    }

    public static ColumnMetadata getLDCInternalData(Category category) {
        ColumnMetadata data = new ColumnMetadata();
        data.setAttrName("LDC Internal");
        data.setEntity(BusinessEntity.Account);
        data.setCanInternalEnrich(true);

        data.setCategory(category);
        data.setDescription(
                "The Intent Score of the business shown in the topic - Investor Relations. Normalized between from 0-100, with greater than 60 meaning a surge in the topic and lesser than 60 meaning average to low intent based on the score.");
        data.setSubcategory("Corporate Finance");
        data.setAttrState(AttrState.Inactive);
        data.setDisplayName("Investor Relations Raw Score");
        data.enableGroup(ColumnSelection.Predefined.Enrichment);
        data.enableGroup(ColumnSelection.Predefined.CompanyProfile);
        data.enableGroup(ColumnSelection.Predefined.TalkingPoint);
        return data;
    }

    public static AttrConfig getCDLStdAttr(Category category, boolean enableThisAttr, boolean useForSegment,
            boolean useForEnrichment, boolean useForCompanyProfile, boolean useForTalkingPoint) {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("CDL Standard");
        attrConfig.setAttrType(AttrType.Custom);
        attrConfig.setAttrSubType(AttrSubType.Standard);
        attrConfig.setEntity(BusinessEntity.Account);

        AttrSpecification attrSpec = AttrSpecification.getAttrSpecification(attrConfig.getAttrType(),
                attrConfig.getAttrSubType(), attrConfig.getEntity());
        AttrConfigProp<Boolean> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(attrSpec.enrichmentChange());
        prop1.setSystemValue(true);
        if (useForEnrichment) {
            prop1.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Enrichment.getName(), prop1);

        AttrConfigProp<Boolean> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(attrSpec.companyProfileChange());
        prop2.setSystemValue(true);
        if (useForCompanyProfile) {
            prop2.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.CompanyProfile.getName(), prop2);

        AttrConfigProp<Category> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(attrSpec.categoryNameChange());
        prop3.setSystemValue(category);
        attrConfig.putProperty(ColumnMetadataKey.Category, prop3);

        AttrConfigProp<String> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(attrSpec.descriptionChange());
        prop4.setSystemValue(
                "The Bucket Code determines the confidence of the Intent Score in the topic - Exclusive Provider Organization (EPO). Has three values, A - high confidence, B - average confidence, and C - low confidence.");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(attrSpec.categoryNameChange());
        prop5.setSystemValue("Health Insurance");
        attrConfig.putProperty(ColumnMetadataKey.Subcategory, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(attrSpec.talkingPointChange());
        prop6.setSystemValue(true);
        if (useForTalkingPoint) {
            prop6.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);

        AttrConfigProp<AttrState> prop7 = new AttrConfigProp<>();
        prop7.setAllowCustomization(attrSpec.stateChange());
        prop7.setSystemValue(AttrState.Inactive);
        if (enableThisAttr) {
            prop7.setCustomValue(AttrState.Active);
        }
        attrConfig.putProperty(ColumnMetadataKey.State, prop7);

        AttrConfigProp<String> prop8 = new AttrConfigProp<>();
        prop8.setAllowCustomization(attrSpec.displayNameChange());
        prop8.setSystemValue("Exclusive Provider Organization (EPO) Confidence");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop8);

        AttrConfigProp<Boolean> prop9 = new AttrConfigProp<>();
        prop9.setAllowCustomization(attrSpec.modelChange());
        prop9.setSystemValue(false);
        attrConfig.putProperty(ColumnSelection.Predefined.Model.name(), prop9);

        AttrConfigProp<Boolean> prop10 = new AttrConfigProp<>();
        prop10.setAllowCustomization(attrSpec.segmentationChange());
        prop10.setSystemValue(false);
        if (useForSegment) {
            prop10.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop10);

        return attrConfig;
    }

    public static ColumnMetadata getCDLStdData(Category category) {
        ColumnMetadata data = new ColumnMetadata();
        data.setAttrName("CDL Standard");
        data.setEntity(BusinessEntity.Account);
        data.setCanInternalEnrich(true);

        data.setCategory(category);
        data.setDescription(
                "The Bucket Code determines the confidence of the Intent Score in the topic - Exclusive Provider Organization (EPO). Has three values, A - high confidence, B - average confidence, and C - low confidence.");
        data.setSubcategory("Health Insurance");
        data.setAttrState(AttrState.Inactive);
        data.setDisplayName("Exclusive Provider Organization (EPO) Confidence");
        data.enableGroup(ColumnSelection.Predefined.Enrichment);
        data.enableGroup(ColumnSelection.Predefined.CompanyProfile);
        data.enableGroup(ColumnSelection.Predefined.TalkingPoint);
        return data;
    }

    public static AttrConfig getCDLLookIDAttr(Category category, boolean enableThisAttr, boolean useForSegment,
            boolean useForEnrichment, boolean useForCompanyProfile, boolean useForTalkingPoint) {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("CDL Lookup Ids");
        attrConfig.setAttrType(AttrType.Custom);
        attrConfig.setAttrSubType(AttrSubType.LookupId);
        attrConfig.setEntity(BusinessEntity.Account);

        AttrSpecification attrSpec = AttrSpecification.getAttrSpecification(attrConfig.getAttrType(),
                attrConfig.getAttrSubType(), attrConfig.getEntity());
        AttrConfigProp<Boolean> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(attrSpec.enrichmentChange());
        prop1.setSystemValue(true);
        if (useForEnrichment) {
            prop1.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Enrichment.getName(), prop1);

        AttrConfigProp<Boolean> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(attrSpec.companyProfileChange());
        prop2.setSystemValue(true);
        if (useForCompanyProfile) {
            prop2.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.CompanyProfile.getName(), prop2);

        AttrConfigProp<Category> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(attrSpec.categoryNameChange());
        prop3.setSystemValue(category);
        attrConfig.putProperty(ColumnMetadataKey.Category, prop3);

        AttrConfigProp<String> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(attrSpec.descriptionChange());
        prop4.setSystemValue(
                "The Bucket Code determines the confidence of the Intent Score in the topic - OAuth 2. Has three values, A - high confidence, B - average confidence, and C - low confidence.");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(attrSpec.categoryNameChange());
        prop5.setSystemValue("Web");
        attrConfig.putProperty(ColumnMetadataKey.Subcategory, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(attrSpec.talkingPointChange());
        prop6.setSystemValue(true);
        if (useForTalkingPoint) {
            prop6.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);

        AttrConfigProp<AttrState> prop7 = new AttrConfigProp<>();
        prop7.setAllowCustomization(attrSpec.stateChange());
        prop7.setSystemValue(AttrState.Inactive);
        if (enableThisAttr) {
            prop7.setCustomValue(AttrState.Active);
        }
        attrConfig.putProperty(ColumnMetadataKey.State, prop7);

        AttrConfigProp<String> prop8 = new AttrConfigProp<>();
        prop8.setAllowCustomization(attrSpec.displayNameChange());
        prop8.setSystemValue("OAuth 2 Confidence");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop8);

        AttrConfigProp<Boolean> prop9 = new AttrConfigProp<>();
        prop9.setAllowCustomization(attrSpec.modelChange());
        prop9.setSystemValue(false);
        attrConfig.putProperty(ColumnSelection.Predefined.Model.getName(), prop9);

        AttrConfigProp<Boolean> prop10 = new AttrConfigProp<>();
        prop10.setAllowCustomization(attrSpec.segmentationChange());
        prop10.setSystemValue(false);
        if (useForSegment) {
            prop10.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop10);

        return attrConfig;
    }

    public static ColumnMetadata getCDLLookIDData(Category category) {
        ColumnMetadata data = new ColumnMetadata();
        data.setAttrName("CDL Lookup Ids");
        data.setEntity(BusinessEntity.Account);
        data.setCanInternalEnrich(true);

        data.setCategory(category);
        data.setDescription(
                "The Bucket Code determines the confidence of the Intent Score in the topic - OAuth 2. Has three values, A - high confidence, B - average confidence, and C - low confidence.");
        data.setSubcategory("Web");
        data.setAttrState(AttrState.Inactive);
        data.setDisplayName("OAuth 2 Confidence");
        data.enableGroup(ColumnSelection.Predefined.Enrichment);
        data.enableGroup(ColumnSelection.Predefined.CompanyProfile);
        data.enableGroup(ColumnSelection.Predefined.TalkingPoint);
        return data;
    }

    public static AttrConfig getCDLAccountExtensionAttr(Category category, boolean enableThisAttr,
            boolean useForSegment, boolean useForEnrichment, boolean useForCompanyProfile, boolean useForTalkingPoint) {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("CDL Account Extension");
        attrConfig.setAttrType(AttrType.Custom);
        attrConfig.setAttrSubType(AttrSubType.Extension);
        attrConfig.setEntity(BusinessEntity.Account);

        AttrSpecification attrSpec = AttrSpecification.getAttrSpecification(attrConfig.getAttrType(),
                attrConfig.getAttrSubType(), attrConfig.getEntity());
        AttrConfigProp<Boolean> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(attrSpec.enrichmentChange());
        prop1.setSystemValue(true);
        if (useForEnrichment) {
            prop1.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Enrichment.getName(), prop1);

        AttrConfigProp<Boolean> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(attrSpec.companyProfileChange());
        prop2.setSystemValue(true);
        if (useForCompanyProfile) {
            prop2.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.CompanyProfile.getName(), prop2);

        AttrConfigProp<Category> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(attrSpec.categoryNameChange());
        prop3.setSystemValue(category);
        attrConfig.putProperty(ColumnMetadataKey.Category, prop3);

        AttrConfigProp<String> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(attrSpec.descriptionChange());
        prop4.setSystemValue(
                "The Bucket Code determines the confidence of the Intent Score in the topic - Source Code Analysis. Has three values, A - high confidence, B - average confidence, and C - low confidence.");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(attrSpec.categoryNameChange());
        prop5.setSystemValue("Software Engineering");
        attrConfig.putProperty(ColumnMetadataKey.Subcategory, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(attrSpec.talkingPointChange());
        prop6.setSystemValue(true);
        if (useForTalkingPoint) {
            prop6.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);

        AttrConfigProp<AttrState> prop7 = new AttrConfigProp<>();
        prop7.setAllowCustomization(attrSpec.stateChange());
        prop7.setSystemValue(AttrState.Inactive);
        if (enableThisAttr) {
            prop7.setCustomValue(AttrState.Active);
        }
        attrConfig.putProperty(ColumnMetadataKey.State, prop7);

        AttrConfigProp<String> prop8 = new AttrConfigProp<>();
        prop8.setAllowCustomization(attrSpec.displayNameChange());
        prop8.setSystemValue("Source Code Analysis Confidence");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop8);

        AttrConfigProp<Boolean> prop9 = new AttrConfigProp<>();
        prop9.setAllowCustomization(attrSpec.modelChange());
        prop9.setSystemValue(false);
        attrConfig.putProperty(ColumnSelection.Predefined.Model.name(), prop9);

        AttrConfigProp<Boolean> prop10 = new AttrConfigProp<>();
        prop10.setAllowCustomization(attrSpec.segmentationChange());
        prop10.setSystemValue(false);
        if (useForSegment) {
            prop10.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop10);

        return attrConfig;
    }

    public static ColumnMetadata getCDLAccountExtensionData(Category category) {
        ColumnMetadata data = new ColumnMetadata();
        data.setAttrName("CDL Account Extensions");
        data.setEntity(BusinessEntity.Account);
        data.setCanInternalEnrich(true);

        data.setCategory(category);
        data.setDescription(
                "The Bucket Code determines the confidence of the Intent Score in the topic - Source Code Analysis. Has three values, A - high confidence, B - average confidence, and C - low confidence.");
        data.setSubcategory("Software Engineering");
        data.setAttrState(AttrState.Inactive);
        data.setDisplayName("Source Code Analysis Confidence");
        data.enableGroup(ColumnSelection.Predefined.Enrichment);
        data.enableGroup(ColumnSelection.Predefined.CompanyProfile);
        data.enableGroup(ColumnSelection.Predefined.TalkingPoint);
        return data;
    }

    public static AttrConfig getCDLContactExtensionAttr(Category category, boolean enableThisAttr,
            boolean useForSegment, boolean useForEnrichment, boolean useForCompanyProfile, boolean useForTalkingPoint) {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("CDL Contact Extension");
        attrConfig.setAttrType(AttrType.Custom);
        attrConfig.setAttrSubType(AttrSubType.Extension);
        attrConfig.setEntity(BusinessEntity.Contact);

        AttrSpecification attrSpec = AttrSpecification.getAttrSpecification(attrConfig.getAttrType(),
                attrConfig.getAttrSubType(), attrConfig.getEntity());
        AttrConfigProp<Boolean> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(attrSpec.enrichmentChange());
        prop1.setSystemValue(true);
        if (useForEnrichment) {
            prop1.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Enrichment.getName(), prop1);

        AttrConfigProp<Boolean> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(attrSpec.companyProfileChange());
        prop2.setSystemValue(true);
        if (useForCompanyProfile) {
            prop2.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.CompanyProfile.getName(), prop2);

        AttrConfigProp<Category> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(attrSpec.categoryNameChange());
        prop3.setSystemValue(category);
        attrConfig.putProperty(ColumnMetadataKey.Category, prop3);

        AttrConfigProp<String> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(attrSpec.descriptionChange());
        prop4.setSystemValue("he Bucket Code determines the confidence of the Intent Score in the topic");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(attrSpec.categoryNameChange());
        prop5.setSystemValue("Strategy & Analysis");
        attrConfig.putProperty(ColumnMetadataKey.Subcategory, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(attrSpec.talkingPointChange());
        prop6.setSystemValue(true);
        if (useForTalkingPoint) {
            prop6.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);

        AttrConfigProp<AttrState> prop7 = new AttrConfigProp<>();
        prop7.setAllowCustomization(attrSpec.stateChange());
        prop7.setSystemValue(AttrState.Inactive);
        if (enableThisAttr) {
            prop7.setCustomValue(AttrState.Active);
        }
        attrConfig.putProperty(ColumnMetadataKey.State, prop7);

        AttrConfigProp<String> prop8 = new AttrConfigProp<>();
        prop8.setAllowCustomization(attrSpec.displayNameChange());
        prop8.setSystemValue("Marketing Attribution Confidence");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop8);

        AttrConfigProp<Boolean> prop9 = new AttrConfigProp<>();
        prop9.setAllowCustomization(attrSpec.modelChange());
        prop9.setSystemValue(false);
        attrConfig.putProperty(ColumnSelection.Predefined.Model.getName(), prop9);

        AttrConfigProp<Boolean> prop10 = new AttrConfigProp<>();
        prop10.setAllowCustomization(attrSpec.segmentationChange());
        prop10.setSystemValue(false);
        if (useForSegment) {
            prop10.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop10);

        return attrConfig;
    }

    public static ColumnMetadata getCDLContactExtensionData(Category category) {
        ColumnMetadata data = new ColumnMetadata();
        data.setAttrName("CDL Contact Extension");
        data.setEntity(BusinessEntity.Account);
        data.setCanInternalEnrich(true);

        data.setCategory(category);
        data.setDescription("he Bucket Code determines the confidence of the Intent Score in the topic");
        data.setSubcategory("Strategy & Analysis");
        data.setAttrState(AttrState.Inactive);
        data.setDisplayName("Marketing Attribution Confidence");
        data.enableGroup(ColumnSelection.Predefined.Enrichment);
        data.enableGroup(ColumnSelection.Predefined.CompanyProfile);
        data.enableGroup(ColumnSelection.Predefined.TalkingPoint);
        return data;
    }

    public static AttrConfig getCDLDerivedPBAttr(Category category, boolean enableThisAttr, boolean useForSegment,
            boolean useForEnrichment, boolean useForCompanyProfile, boolean useForTalkingPoint) {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("CDL Derived PB");
        attrConfig.setAttrType(AttrType.Curated);
        attrConfig.setAttrSubType(AttrSubType.ProductBundle);
        attrConfig.setEntity(BusinessEntity.Account);

        AttrSpecification attrSpec = AttrSpecification.getAttrSpecification(attrConfig.getAttrType(),
                attrConfig.getAttrSubType(), attrConfig.getEntity());
        AttrConfigProp<Boolean> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(attrSpec.enrichmentChange());
        prop1.setSystemValue(true);
        if (useForEnrichment) {
            prop1.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Enrichment.getName(), prop1);

        AttrConfigProp<Boolean> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(attrSpec.companyProfileChange());
        prop2.setSystemValue(true);
        if (useForCompanyProfile) {
            prop2.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.CompanyProfile.getName(), prop2);

        AttrConfigProp<Category> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(attrSpec.categoryNameChange());
        prop3.setSystemValue(category);
        attrConfig.putProperty(ColumnMetadataKey.Category, prop3);

        AttrConfigProp<String> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(attrSpec.descriptionChange());
        prop4.setSystemValue("The Intent Score of the business shown in the topic.");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(attrSpec.categoryNameChange());
        prop5.setSystemValue("Campaigns");
        attrConfig.putProperty(ColumnMetadataKey.Subcategory, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(attrSpec.talkingPointChange());
        prop6.setSystemValue(true);
        if (useForTalkingPoint) {
            prop6.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);

        AttrConfigProp<AttrState> prop7 = new AttrConfigProp<>();
        prop7.setAllowCustomization(attrSpec.stateChange());
        prop7.setSystemValue(AttrState.Inactive);
        if (enableThisAttr) {
            prop7.setCustomValue(AttrState.Active);
        }
        attrConfig.putProperty(ColumnMetadataKey.State, prop7);

        AttrConfigProp<String> prop8 = new AttrConfigProp<>();
        prop8.setAllowCustomization(attrSpec.displayNameChange());
        prop8.setSystemValue("Creative Services Raw Score");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop8);

        AttrConfigProp<Boolean> prop9 = new AttrConfigProp<>();
        prop9.setAllowCustomization(attrSpec.modelChange());
        prop9.setSystemValue(false);
        attrConfig.putProperty(ColumnSelection.Predefined.Model.name(), prop9);

        AttrConfigProp<Boolean> prop10 = new AttrConfigProp<>();
        prop10.setAllowCustomization(attrSpec.segmentationChange());
        prop10.setSystemValue(false);
        if (useForSegment) {
            prop10.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop10);

        return attrConfig;
    }

    public static ColumnMetadata getCDLDerivedPBData(Category category) {
        ColumnMetadata data = new ColumnMetadata();
        data.setAttrName("CDL Derived PB");
        data.setEntity(BusinessEntity.Account);
        data.setCanInternalEnrich(true);

        data.setCategory(category);
        data.setDescription("The Intent Score of the business shown in the topic.");
        data.setSubcategory("Campaigns");
        data.setAttrState(AttrState.Inactive);
        data.setDisplayName("Creative Services Raw Score");
        data.enableGroup(ColumnSelection.Predefined.Enrichment);
        data.enableGroup(ColumnSelection.Predefined.CompanyProfile);
        data.enableGroup(ColumnSelection.Predefined.TalkingPoint);
        return data;
    }

    public static AttrConfig getCDLRatingAttr(Category category, boolean enableThisAttr, boolean useForSegment,
            boolean useForEnrichment, boolean useForCompanyProfile, boolean useForTalkingPoint) {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("CDL Rating");
        attrConfig.setAttrType(AttrType.Curated);
        attrConfig.setAttrSubType(AttrSubType.Rating);
        attrConfig.setEntity(BusinessEntity.Account);

        AttrSpecification attrSpec = AttrSpecification.getAttrSpecification(attrConfig.getAttrType(),
                attrConfig.getAttrSubType(), attrConfig.getEntity());
        AttrConfigProp<Boolean> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(attrSpec.enrichmentChange());
        prop1.setSystemValue(true);
        if (useForEnrichment) {
            prop1.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Enrichment.getName(), prop1);

        AttrConfigProp<Boolean> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(attrSpec.companyProfileChange());
        prop2.setSystemValue(true);
        if (useForCompanyProfile) {
            prop2.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.CompanyProfile.getName(), prop2);

        AttrConfigProp<Category> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(attrSpec.categoryNameChange());
        prop3.setSystemValue(category);
        attrConfig.putProperty(ColumnMetadataKey.Category, prop3);

        AttrConfigProp<String> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(attrSpec.descriptionChange());
        prop4.setSystemValue("The Bucket Code determines the confidence of the Intent Score in the topic.");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(attrSpec.categoryNameChange());
        prop5.setSystemValue("Software Engineering");
        attrConfig.putProperty(ColumnMetadataKey.Subcategory, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(attrSpec.talkingPointChange());
        prop6.setSystemValue(true);
        if (useForTalkingPoint) {
            prop6.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);

        AttrConfigProp<AttrState> prop7 = new AttrConfigProp<>();
        prop7.setAllowCustomization(attrSpec.stateChange());
        prop7.setSystemValue(AttrState.Inactive);
        if (enableThisAttr) {
            prop7.setCustomValue(AttrState.Active);
        }
        attrConfig.putProperty(ColumnMetadataKey.State, prop7);

        AttrConfigProp<String> prop8 = new AttrConfigProp<>();
        prop8.setAllowCustomization(attrSpec.displayNameChange());
        prop8.setSystemValue("Knowledge Management Software Confidence");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop8);

        AttrConfigProp<Boolean> prop9 = new AttrConfigProp<>();
        prop9.setAllowCustomization(attrSpec.modelChange());
        prop9.setSystemValue(false);
        attrConfig.putProperty(ColumnSelection.Predefined.Model.name(), prop9);

        AttrConfigProp<Boolean> prop10 = new AttrConfigProp<>();
        prop10.setAllowCustomization(attrSpec.segmentationChange());
        prop10.setSystemValue(false);
        if (useForSegment) {
            prop10.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop10);

        return attrConfig;
    }

    public static ColumnMetadata getCDLRatingData(Category category) {
        ColumnMetadata data = new ColumnMetadata();
        data.setAttrName("CDL Rating");
        data.setEntity(BusinessEntity.Account);
        data.setCanInternalEnrich(true);

        data.setCategory(category);
        data.setDescription("The Bucket Code determines the confidence of the Intent Score in the topic.");
        data.setSubcategory("Software Engineering");
        data.setAttrState(AttrState.Inactive);
        data.setDisplayName("Knowledge Management Software Confidence");
        data.enableGroup(ColumnSelection.Predefined.Enrichment);
        data.enableGroup(ColumnSelection.Predefined.CompanyProfile);
        data.enableGroup(ColumnSelection.Predefined.TalkingPoint);
        return data;
    }

    public static AttrConfig getAccountId() {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName(InterfaceName.AccountId.name());
        attrConfig.setAttrType(AttrType.Internal);
        attrConfig.setEntity(BusinessEntity.Account);
        AttrConfigProp<String> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(true);
        prop1.setSystemValue("AccountId");
        prop1.setCustomValue("ID");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop1);

        AttrConfigProp<String> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(true);
        prop2.setSystemValue("Category1");
        prop2.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.Category, prop2);

        AttrConfigProp<String> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(true);
        prop3.setSystemValue("Account ID");
        prop3.setCustomValue("Internal Account ID");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop3);

        AttrConfigProp<AttrState> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(true);
        prop4.setSystemValue(AttrState.Active);
        prop4.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.State, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(false);
        prop5.setSystemValue("NONE");
        prop5.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.ApprovedUsage, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(true);
        prop6.setSystemValue(true);
        prop6.setCustomValue(null);
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);
        return attrConfig;
    }

    public static ColumnMetadata getAccountIdData(Category category) {
        ColumnMetadata data = new ColumnMetadata();
        data.setAttrName(InterfaceName.LatticeAccountId.name());
        data.setEntity(BusinessEntity.Account);

        data.setCategory(category);
        data.setDescription("Account ID");
        data.setAttrState(AttrState.Active);
        data.setDisplayName("AccountId");
        data.enableGroup(ColumnSelection.Predefined.TalkingPoint);
        return data;
    }

    public static ColumnMetadata getContactAccountIdData(Category category) {
        ColumnMetadata data = new ColumnMetadata();
        data.setAttrName(InterfaceName.CustomerAccountId.name());
        data.setEntity(BusinessEntity.Contact);

        data.setCategory(category);
        data.setDescription("Customer provided Account ID in Contact");
        data.setAttrState(AttrState.Active);
        data.setDisplayName("AccountId");
        data.enableGroup(ColumnSelection.Predefined.TalkingPoint);
        return data;
    }

    public static AttrConfig getAnnualRevenue() {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName(InterfaceName.AnnualRevenue.name());
        attrConfig.setAttrType(AttrType.Internal);
        attrConfig.setEntity(BusinessEntity.Account);
        AttrConfigProp<String> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(true);
        prop1.setSystemValue("AnnualRevenue");
        prop1.setCustomValue("Annual_Revenue");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop1);

        AttrConfigProp<String> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(true);
        prop2.setSystemValue("Category2");
        prop2.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.Category, prop2);

        AttrConfigProp<String> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(true);
        prop3.setSystemValue(null);
        prop3.setCustomValue("AnnualRevenue");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop3);

        AttrConfigProp<AttrState> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(true);
        prop4.setSystemValue(AttrState.Inactive);
        prop4.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.State, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(false);
        prop5.setSystemValue("ModelAndAllInsigts");
        prop5.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.ApprovedUsage, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(true);
        prop6.setSystemValue(true);
        prop6.setCustomValue(null);
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);
        return attrConfig;
    }

    public static AttrConfig getCustomeAccountAttr() {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("Email");
        attrConfig.setAttrType(AttrType.Custom);
        attrConfig.setEntity(BusinessEntity.Account);
        AttrConfigProp<String> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(true);
        prop1.setSystemValue("Email");
        prop1.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop1);

        AttrConfigProp<String> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(true);
        prop2.setSystemValue("Category2");
        prop2.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.Category, prop2);

        AttrConfigProp<String> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(true);
        prop3.setSystemValue("Email");
        prop3.setCustomValue("Email Address");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop3);

        AttrConfigProp<AttrState> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(true);
        prop4.setSystemValue(AttrState.Active);
        prop4.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.State, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(false);
        prop5.setSystemValue("None");
        prop5.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.ApprovedUsage, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(true);
        prop6.setSystemValue(true);
        prop6.setCustomValue(null);
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);
        return attrConfig;
    }

    public static AttrConfig getContactId() {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName(InterfaceName.ContactId.name());
        attrConfig.setAttrType(AttrType.Internal);
        attrConfig.setEntity(BusinessEntity.Contact);
        AttrConfigProp<String> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(true);
        prop1.setSystemValue("ContactId");
        prop1.setCustomValue("Contact_Id");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop1);

        AttrConfigProp<String> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(true);
        prop2.setSystemValue("Category1");
        prop2.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.Category, prop2);

        AttrConfigProp<String> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(true);
        prop3.setSystemValue("ContactID");
        prop3.setCustomValue("Contact ID");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop3);

        AttrConfigProp<AttrState> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(true);
        prop4.setSystemValue(AttrState.Active);
        prop4.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.State, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(false);
        prop5.setSystemValue("None");
        prop5.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.ApprovedUsage, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(true);
        prop6.setSystemValue(true);
        prop6.setCustomValue(null);
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop6);
        return attrConfig;
    }

    public static AttrConfig getContactFirstName() {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName(InterfaceName.FirstName.name());
        attrConfig.setAttrType(AttrType.Internal);
        attrConfig.setEntity(BusinessEntity.Contact);
        AttrConfigProp<String> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(true);
        prop1.setSystemValue("FirstName");
        prop1.setCustomValue("First Name");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop1);

        AttrConfigProp<String> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(true);
        prop2.setSystemValue("Category1");
        prop2.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.Category, prop2);

        AttrConfigProp<String> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(true);
        prop3.setSystemValue(null);
        prop3.setCustomValue("First Name");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop3);

        AttrConfigProp<AttrState> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(true);
        prop4.setSystemValue(AttrState.Active);
        prop4.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.State, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(false);
        prop5.setSystemValue("None");
        prop5.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.ApprovedUsage, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(true);
        prop6.setSystemValue(true);
        prop6.setCustomValue(null);
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop6);
        return attrConfig;
    }

    public static List<AttrConfig> generatePropertyList(Category category, boolean state, boolean useForSegment,
            boolean useForEnrichment, boolean useForCompanyProfile, boolean useForTalkingPoint,
            boolean deprecateLDCNonPremiumAttr) {
        List<AttrConfig> renderedList = Arrays.asList(
                AttrConfigTestUtils.getLDCNonPremiumAttr(category, state, useForSegment, useForEnrichment,
                        useForCompanyProfile, useForTalkingPoint),
                AttrConfigTestUtils.getLDCPremiumAttr(category, state, useForSegment, useForEnrichment,
                        useForCompanyProfile, useForTalkingPoint), //
                AttrConfigTestUtils.getLDCInternalAttr(category, state, useForSegment, useForEnrichment,
                        useForCompanyProfile, useForTalkingPoint), //
                AttrConfigTestUtils.getCDLStdAttr(category, state, useForSegment, useForEnrichment,
                        useForCompanyProfile, useForTalkingPoint), //
                AttrConfigTestUtils.getCDLLookIDAttr(category, state, useForSegment, useForEnrichment,
                        useForCompanyProfile, useForTalkingPoint), //
                AttrConfigTestUtils.getCDLAccountExtensionAttr(category, state, useForSegment, useForEnrichment,
                        useForCompanyProfile, useForTalkingPoint), //
                AttrConfigTestUtils.getCDLContactExtensionAttr(category, state, useForSegment, useForEnrichment,
                        useForCompanyProfile, useForTalkingPoint), //
                AttrConfigTestUtils.getCDLDerivedPBAttr(category, state, useForSegment, useForEnrichment,
                        useForCompanyProfile, useForTalkingPoint), //
                AttrConfigTestUtils.getCDLRatingAttr(category, state, useForSegment, useForEnrichment,
                        useForCompanyProfile, useForTalkingPoint));
        renderedList.get(0).setShouldDeprecate(deprecateLDCNonPremiumAttr);
        return renderedList;
    }

    public static List<AttrConfig> generatePropertyList(Category category, boolean state, boolean useForSegment,
            boolean useForEnrichment, boolean useForCompanyProfile, boolean useForTalkingPoint) {
        return generatePropertyList(category, state, useForSegment, useForEnrichment, useForCompanyProfile,
                useForTalkingPoint, false);
    }

    public static int getErrorNumber(List<AttrConfig> configs) {
        int numErrors = 0;
        for (AttrConfig config : configs) {
            if (config.getValidationErrors() != null) {
                numErrors++;
            }
        }
        return numErrors;
    }

}
