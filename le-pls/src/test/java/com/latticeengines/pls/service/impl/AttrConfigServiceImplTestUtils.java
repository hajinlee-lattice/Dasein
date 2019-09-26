package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigCategoryOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceapps.core.AttrSubType;
import com.latticeengines.domain.exposed.serviceapps.core.AttrType;
import com.latticeengines.domain.exposed.serviceapps.core.ImpactWarnings;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails.AttrValidation;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationErrors;

public class AttrConfigServiceImplTestUtils {
    private static final Logger log = LoggerFactory.getLogger(AttrConfigServiceImplTestUtils.class);

    public static final String[] select = { "attr1", "attr2", "attr3" };
    public static final String[] deselect = { "attr4", "attr5", "attr6" };
    public static final Long intentLimit = 500L;
    public static final Long activeForIntent = 5000L;
    public static final Long inactiveForIntent = 4000L;
    public static final Long totalIntentAttrs = 90000L;
    public static final Long tpLimit = 500L;
    public static final Long activeForTp = 5000L;
    public static final Long inactiveForTp = 4000L;
    public static final Long totalTpAttrs = 90000L;
    public static final Long accountLimit = 500L;
    public static final Long activeForAccount = 5000L;
    public static final Long inactiveForAccount = 4000L;
    public static final Long totalAccountAttrs = 90000L;
    public static final Long contactLimit = 500L;
    public static final Long activeForContact = 5000L;
    public static final Long inactiveForContact = 4000L;
    public static final Long totalContactAttrs = 90000L;
    public static final Long websiteKeywordLimit = 500L;
    public static final Long activeForWebsiteKeyword = 5000L;
    public static final Long inactiveForWebsiteKeyword = 4000L;
    public static final Long totalWebsiteKeywordAttrs = 90000L;

    public static AttrConfig getAttr1(Category category, boolean enableThisAttr) {
        return getAttr1(category, enableThisAttr, false);
    }

    public static AttrConfig getAttr2(Category category, boolean enableThisAttr) {
        return getAttr2(category, enableThisAttr, false);
    }

    public static AttrConfig getAttr3(Category category, boolean enableThisAttr) {
        return getAttr3(category, enableThisAttr, false);
    }

    public static AttrConfig getAttr4(Category category, boolean enableThisAttr) {
        return getAttr4(category, enableThisAttr, false);
    }

    public static AttrConfig getAttr5(Category category, boolean enableThisAttr) {
        return getAttr5(category, enableThisAttr, false);
    }

    public static AttrConfig getAttr6(Category category, boolean enableThisAttr) {
        return getAttr6(category, enableThisAttr, false);
    }

    public static AttrConfig getAttr7(Category category, boolean enableThisAttr) {
        return getAttr7(category, enableThisAttr, false);
    }

    public static AttrConfig getAttr8(Category category, boolean enableThisAttr) {
        return getAttr8(category, enableThisAttr, false);
    }

    public static AttrConfig getAttr9(Category category, boolean enableThisAttr) {
        return getAttr9(category, enableThisAttr, false);
    }

    public static AttrConfig getAttr1(Category category, boolean enableThisAttr, boolean useForSegment) {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("Attr1");
        attrConfig.setAttrType(AttrType.DataCloud);
        attrConfig.setAttrSubType(AttrSubType.Normal);
        attrConfig.setEntity(BusinessEntity.Account);

        AttrConfigProp<Boolean> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(true);
        prop1.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.Enrichment.getName(), prop1);

        AttrConfigProp<Boolean> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(true);
        prop2.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.CompanyProfile.getName(), prop2);

        AttrConfigProp<String> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(true);
        prop3.setSystemValue(category.getName());
        attrConfig.putProperty(ColumnMetadataKey.Category, prop3);

        AttrConfigProp<String> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(false);
        prop4.setSystemValue(
                "Changes during the last 30 days in users showing intent activity on Recruiting and Hiring");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(true);
        prop5.setSystemValue("Other");
        attrConfig.putProperty(ColumnMetadataKey.Subcategory, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(true);
        prop6.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);

        AttrConfigProp<AttrState> prop7 = new AttrConfigProp<>();
        prop7.setAllowCustomization(true);
        prop7.setSystemValue(AttrState.Inactive);
        if (enableThisAttr) {
            prop7.setCustomValue(AttrState.Active);
        }
        attrConfig.putProperty(ColumnMetadataKey.State, prop7);

        AttrConfigProp<String> prop8 = new AttrConfigProp<>();
        prop8.setAllowCustomization(true);
        prop8.setSystemValue("Recruiting & Hiring Intent Users Change (Deprecated)");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop8);

        AttrConfigProp<Boolean> prop9 = new AttrConfigProp<>();
        prop9.setAllowCustomization(true);
        prop9.setSystemValue(false);
        attrConfig.putProperty(ColumnSelection.Predefined.Model.getName(), prop9);

        AttrConfigProp<Boolean> prop10 = new AttrConfigProp<>();
        prop10.setAllowCustomization(true);
        prop10.setSystemValue(false);
        if (useForSegment) {
            prop10.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop10);

        return attrConfig;
    }

    public static AttrConfig getAttr2(Category category, boolean enableThisAttr, boolean useForSegment) {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("Attr2");
        attrConfig.setAttrType(AttrType.DataCloud);
        attrConfig.setAttrSubType(AttrSubType.InternalEnrich);
        attrConfig.setEntity(BusinessEntity.Account);

        AttrConfigProp<Boolean> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(true);
        prop1.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.Enrichment.getName(), prop1);

        AttrConfigProp<Boolean> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(false);
        prop2.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.CompanyProfile.getName(), prop2);

        AttrConfigProp<String> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(true);
        prop3.setSystemValue(category.getName());
        attrConfig.putProperty(ColumnMetadataKey.Category, prop3);

        AttrConfigProp<String> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(false);
        prop4.setSystemValue(
                "The Intent Score of the business shown in the topic - Permission Email Marketing. Normalized between from 0-100, with greater than 60 meaning a surge in the topic and lesser than 60 meaning average to low intent based on the score.");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(true);
        prop5.setSystemValue("Email Marketing");
        attrConfig.putProperty(ColumnMetadataKey.Subcategory, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(true);
        prop6.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);

        AttrConfigProp<AttrState> prop7 = new AttrConfigProp<>();
        prop7.setAllowCustomization(true);
        prop7.setSystemValue(AttrState.Inactive);
        if (enableThisAttr) {
            prop7.setCustomValue(AttrState.Active);
        }
        attrConfig.putProperty(ColumnMetadataKey.State, prop7);

        AttrConfigProp<String> prop8 = new AttrConfigProp<>();
        prop8.setAllowCustomization(true);
        prop8.setSystemValue("Permission Email Marketing Raw Score");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop8);

        AttrConfigProp<Boolean> prop9 = new AttrConfigProp<>();
        prop9.setAllowCustomization(true);
        prop9.setSystemValue(false);
        attrConfig.putProperty(ColumnSelection.Predefined.Model.name(), prop9);

        AttrConfigProp<Boolean> prop10 = new AttrConfigProp<>();
        prop10.setAllowCustomization(false);
        prop10.setSystemValue(false);
        if (useForSegment) {
            prop10.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop10);

        return attrConfig;
    }

    public static AttrConfig getAttr3(Category category, boolean enableThisAttr, boolean useForSegment) {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("Attr3");
        attrConfig.setAttrType(AttrType.DataCloud);
        attrConfig.setAttrSubType(AttrSubType.InternalEnrich);
        attrConfig.setEntity(BusinessEntity.Account);

        AttrConfigProp<Boolean> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(true);
        prop1.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.Enrichment.getName(), prop1);

        AttrConfigProp<Boolean> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(false);
        prop2.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.CompanyProfile.getName(), prop2);

        AttrConfigProp<String> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(true);
        prop3.setSystemValue(category.getName());
        attrConfig.putProperty(ColumnMetadataKey.Category, prop3);

        AttrConfigProp<String> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(false);
        prop4.setSystemValue(
                "The Intent Score of the business shown in the topic - Investor Relations. Normalized between from 0-100, with greater than 60 meaning a surge in the topic and lesser than 60 meaning average to low intent based on the score.");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(true);
        prop5.setSystemValue("Corporate Finance");
        attrConfig.putProperty(ColumnMetadataKey.Subcategory, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(true);
        prop6.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);

        AttrConfigProp<AttrState> prop7 = new AttrConfigProp<>();
        prop7.setAllowCustomization(true);
        prop7.setSystemValue(AttrState.Inactive);
        if (enableThisAttr) {
            prop7.setCustomValue(AttrState.Active);
        }
        attrConfig.putProperty(ColumnMetadataKey.State, prop7);

        AttrConfigProp<String> prop8 = new AttrConfigProp<>();
        prop8.setAllowCustomization(false);
        prop8.setSystemValue("Investor Relations Raw Score");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop8);

        AttrConfigProp<Boolean> prop9 = new AttrConfigProp<>();
        prop9.setAllowCustomization(true);
        prop9.setSystemValue(false);
        attrConfig.putProperty(ColumnSelection.Predefined.Model.getName(), prop9);

        AttrConfigProp<Boolean> prop10 = new AttrConfigProp<>();
        prop10.setAllowCustomization(false);
        prop10.setSystemValue(false);
        if (useForSegment) {
            prop10.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop10);

        return attrConfig;
    }

    public static AttrConfig getAttr4(Category category, boolean enableThisAttr, boolean useForSegment) {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("Attr4");
        attrConfig.setAttrType(AttrType.DataCloud);
        attrConfig.setAttrSubType(AttrSubType.InternalEnrich);
        attrConfig.setEntity(BusinessEntity.Account);

        AttrConfigProp<Boolean> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(true);
        prop1.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.Enrichment.getName(), prop1);

        AttrConfigProp<Boolean> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(false);
        prop2.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.CompanyProfile.getName(), prop2);

        AttrConfigProp<String> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(true);
        prop3.setSystemValue(category.getName());
        attrConfig.putProperty(ColumnMetadataKey.Category, prop3);

        AttrConfigProp<String> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(false);
        prop4.setSystemValue(
                "The Bucket Code determines the confidence of the Intent Score in the topic - Exclusive Provider Organization (EPO). Has three values, A - high confidence, B - average confidence, and C - low confidence.");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(true);
        prop5.setSystemValue("Health Insurance");
        attrConfig.putProperty(ColumnMetadataKey.Subcategory, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(true);
        prop6.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);

        AttrConfigProp<AttrState> prop7 = new AttrConfigProp<>();
        prop7.setAllowCustomization(true);
        prop7.setSystemValue(AttrState.Inactive);
        if (enableThisAttr) {
            prop7.setCustomValue(AttrState.Active);
        }
        attrConfig.putProperty(ColumnMetadataKey.State, prop7);

        AttrConfigProp<String> prop8 = new AttrConfigProp<>();
        prop8.setAllowCustomization(false);
        prop8.setSystemValue("Exclusive Provider Organization (EPO) Confidence");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop8);

        AttrConfigProp<Boolean> prop9 = new AttrConfigProp<>();
        prop9.setAllowCustomization(true);
        prop9.setSystemValue(false);
        attrConfig.putProperty(ColumnSelection.Predefined.Model.name(), prop9);

        AttrConfigProp<Boolean> prop10 = new AttrConfigProp<>();
        prop10.setAllowCustomization(false);
        prop10.setSystemValue(false);
        if (useForSegment) {
            prop10.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop10);

        return attrConfig;
    }

    public static AttrConfig getAttr5(Category category, boolean enableThisAttr, boolean useForSegment) {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("Attr5");
        attrConfig.setAttrType(AttrType.DataCloud);
        attrConfig.setAttrSubType(AttrSubType.InternalEnrich);
        attrConfig.setEntity(BusinessEntity.Account);

        AttrConfigProp<Boolean> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(true);
        prop1.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.Enrichment.getName(), prop1);

        AttrConfigProp<Boolean> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(false);
        prop2.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.CompanyProfile.getName(), prop2);

        AttrConfigProp<String> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(true);
        prop3.setSystemValue(category.getName());
        attrConfig.putProperty(ColumnMetadataKey.Category, prop3);

        AttrConfigProp<String> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(false);
        prop4.setSystemValue(
                "The Bucket Code determines the confidence of the Intent Score in the topic - OAuth 2. Has three values, A - high confidence, B - average confidence, and C - low confidence.");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(true);
        prop5.setSystemValue("Web");
        attrConfig.putProperty(ColumnMetadataKey.Subcategory, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(true);
        prop6.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);

        AttrConfigProp<AttrState> prop7 = new AttrConfigProp<>();
        prop7.setAllowCustomization(true);
        prop7.setSystemValue(AttrState.Inactive);
        if (enableThisAttr) {
            prop7.setCustomValue(AttrState.Active);
        }
        attrConfig.putProperty(ColumnMetadataKey.State, prop7);

        AttrConfigProp<String> prop8 = new AttrConfigProp<>();
        prop8.setAllowCustomization(false);
        prop8.setSystemValue("OAuth 2 Confidence");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop8);

        AttrConfigProp<Boolean> prop9 = new AttrConfigProp<>();
        prop9.setAllowCustomization(true);
        prop9.setSystemValue(false);
        attrConfig.putProperty(ColumnSelection.Predefined.Model.getName(), prop9);

        AttrConfigProp<Boolean> prop10 = new AttrConfigProp<>();
        prop10.setAllowCustomization(false);
        prop10.setSystemValue(false);
        if (useForSegment) {
            prop10.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop10);

        return attrConfig;
    }

    public static AttrConfig getAttr6(Category category, boolean enableThisAttr, boolean useForSegment) {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("Attr6");
        attrConfig.setAttrType(AttrType.DataCloud);
        attrConfig.setAttrSubType(AttrSubType.InternalEnrich);
        attrConfig.setEntity(BusinessEntity.Account);

        AttrConfigProp<Boolean> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(true);
        prop1.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.Enrichment.getName(), prop1);

        AttrConfigProp<Boolean> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(false);
        prop2.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.CompanyProfile.getName(), prop2);

        AttrConfigProp<String> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(true);
        prop3.setSystemValue(category.getName());
        attrConfig.putProperty(ColumnMetadataKey.Category, prop3);

        AttrConfigProp<String> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(false);
        prop4.setSystemValue(
                "The Bucket Code determines the confidence of the Intent Score in the topic - Source Code Analysis. Has three values, A - high confidence, B - average confidence, and C - low confidence.");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(true);
        prop5.setSystemValue("Software Engineering");
        attrConfig.putProperty(ColumnMetadataKey.Subcategory, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(true);
        prop6.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);

        AttrConfigProp<AttrState> prop7 = new AttrConfigProp<>();
        prop7.setAllowCustomization(true);
        prop7.setSystemValue(AttrState.Inactive);
        if (enableThisAttr) {
            prop7.setCustomValue(AttrState.Active);
        }
        attrConfig.putProperty(ColumnMetadataKey.State, prop7);

        AttrConfigProp<String> prop8 = new AttrConfigProp<>();
        prop8.setAllowCustomization(false);
        prop8.setSystemValue("Source Code Analysis Confidence");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop8);

        AttrConfigProp<Boolean> prop9 = new AttrConfigProp<>();
        prop9.setAllowCustomization(true);
        prop9.setSystemValue(false);
        attrConfig.putProperty(ColumnSelection.Predefined.Model.name(), prop9);

        AttrConfigProp<Boolean> prop10 = new AttrConfigProp<>();
        prop10.setAllowCustomization(false);
        prop10.setSystemValue(false);
        if (useForSegment) {
            prop10.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop10);

        return attrConfig;
    }

    public static AttrConfig getAttr7(Category category, boolean enableThisAttr, boolean useForSegment) {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("Attr7");
        attrConfig.setAttrType(AttrType.DataCloud);
        attrConfig.setAttrSubType(AttrSubType.InternalEnrich);
        attrConfig.setEntity(BusinessEntity.Account);

        AttrConfigProp<Boolean> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(true);
        prop1.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.Enrichment.getName(), prop1);

        AttrConfigProp<Boolean> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(false);
        prop2.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.CompanyProfile.getName(), prop2);

        AttrConfigProp<String> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(true);
        prop3.setSystemValue(category.getName());
        attrConfig.putProperty(ColumnMetadataKey.Category, prop3);

        AttrConfigProp<String> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(false);
        prop4.setSystemValue("he Bucket Code determines the confidence of the Intent Score in the topic");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(true);
        prop5.setSystemValue("Strategy & Analysis");
        attrConfig.putProperty(ColumnMetadataKey.Subcategory, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(true);
        prop6.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);

        AttrConfigProp<AttrState> prop7 = new AttrConfigProp<>();
        prop7.setAllowCustomization(true);
        prop7.setSystemValue(AttrState.Inactive);
        if (enableThisAttr) {
            prop7.setCustomValue(AttrState.Active);
        }
        attrConfig.putProperty(ColumnMetadataKey.State, prop7);

        AttrConfigProp<String> prop8 = new AttrConfigProp<>();
        prop8.setAllowCustomization(false);
        prop8.setSystemValue("Marketing Attribution Confidence");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop8);

        AttrConfigProp<Boolean> prop9 = new AttrConfigProp<>();
        prop9.setAllowCustomization(true);
        prop9.setSystemValue(false);
        attrConfig.putProperty(ColumnSelection.Predefined.Model.getName(), prop9);

        AttrConfigProp<Boolean> prop10 = new AttrConfigProp<>();
        prop10.setAllowCustomization(false);
        prop10.setSystemValue(false);
        if (useForSegment) {
            prop10.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop10);

        return attrConfig;
    }

    public static AttrConfig getAttr8(Category category, boolean enableThisAttr, boolean useForSegment) {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("Attr8");
        attrConfig.setAttrType(AttrType.DataCloud);
        attrConfig.setAttrSubType(AttrSubType.InternalEnrich);
        attrConfig.setEntity(BusinessEntity.Account);

        AttrConfigProp<Boolean> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(true);
        prop1.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.Enrichment.getName(), prop1);

        AttrConfigProp<Boolean> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(false);
        prop2.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.CompanyProfile.getName(), prop2);

        AttrConfigProp<String> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(true);
        prop3.setSystemValue(category.getName());
        attrConfig.putProperty(ColumnMetadataKey.Category, prop3);

        AttrConfigProp<String> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(false);
        prop4.setSystemValue("The Intent Score of the business shown in the topic.");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(true);
        prop5.setSystemValue("Campaigns");
        attrConfig.putProperty(ColumnMetadataKey.Subcategory, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(true);
        prop6.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);

        AttrConfigProp<AttrState> prop7 = new AttrConfigProp<>();
        prop7.setAllowCustomization(true);
        prop7.setSystemValue(AttrState.Inactive);
        if (enableThisAttr) {
            prop7.setCustomValue(AttrState.Active);
        }
        attrConfig.putProperty(ColumnMetadataKey.State, prop7);

        AttrConfigProp<String> prop8 = new AttrConfigProp<>();
        prop8.setAllowCustomization(false);
        prop8.setSystemValue("Creative Services Raw Score");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop8);

        AttrConfigProp<Boolean> prop9 = new AttrConfigProp<>();
        prop9.setAllowCustomization(true);
        prop9.setSystemValue(false);
        attrConfig.putProperty(ColumnSelection.Predefined.Model.name(), prop9);

        AttrConfigProp<Boolean> prop10 = new AttrConfigProp<>();
        prop10.setAllowCustomization(false);
        prop10.setSystemValue(false);
        if (useForSegment) {
            prop10.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop10);

        return attrConfig;
    }

    public static AttrConfig getAttr9(Category category, boolean enableThisAttr, boolean useForSegment) {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("Attr9");
        attrConfig.setAttrType(AttrType.DataCloud);
        attrConfig.setAttrSubType(AttrSubType.InternalEnrich);
        attrConfig.setEntity(BusinessEntity.Account);

        AttrConfigProp<Boolean> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(true);
        prop1.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.Enrichment.getName(), prop1);

        AttrConfigProp<Boolean> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(false);
        prop2.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.CompanyProfile.getName(), prop2);

        AttrConfigProp<String> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(true);
        prop3.setSystemValue(category.getName());
        attrConfig.putProperty(ColumnMetadataKey.Category, prop3);

        AttrConfigProp<String> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(false);
        prop4.setSystemValue("The Bucket Code determines the confidence of the Intent Score in the topic.");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(true);
        prop5.setSystemValue("Software Engineering");
        attrConfig.putProperty(ColumnMetadataKey.Subcategory, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(true);
        prop6.setSystemValue(true);
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);

        AttrConfigProp<AttrState> prop7 = new AttrConfigProp<>();
        prop7.setAllowCustomization(false);
        prop7.setSystemValue(AttrState.Inactive);
        if (enableThisAttr) {
            prop7.setCustomValue(AttrState.Active);
        }
        attrConfig.putProperty(ColumnMetadataKey.State, prop7);

        AttrConfigProp<String> prop8 = new AttrConfigProp<>();
        prop8.setAllowCustomization(false);
        prop8.setSystemValue("Knowledge Management Software Confidence");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop8);

        AttrConfigProp<Boolean> prop9 = new AttrConfigProp<>();
        prop9.setAllowCustomization(true);
        prop9.setSystemValue(false);
        attrConfig.putProperty(ColumnSelection.Predefined.Model.name(), prop9);

        AttrConfigProp<Boolean> prop10 = new AttrConfigProp<>();
        prop10.setAllowCustomization(false);
        prop10.setSystemValue(false);
        if (useForSegment) {
            prop10.setCustomValue(true);
        }
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop10);

        return attrConfig;
    }

    public static Map<String, AttrConfigCategoryOverview<?>> generatePropertyAttrConfigOverviewForUsage(
            List<String> propertyNames) {
        Map<String, AttrConfigCategoryOverview<?>> result = new HashMap<>();
        AttrConfigCategoryOverview<Boolean> attrConfig1 = new AttrConfigCategoryOverview<>();
        result.put(Category.FIRMOGRAPHICS.getName(), attrConfig1);
        attrConfig1.setLimit(500L);
        attrConfig1.setTotalAttrs(131L);
        Map<String, Map<Boolean, Long>> propSummary1 = new HashMap<>();
        attrConfig1.setPropSummary(propSummary1);
        for (String propertyName : propertyNames) {
            Map<Boolean, Long> propDetails1 = new HashMap<>();
            propDetails1.put(Boolean.FALSE, 46L);
            propDetails1.put(Boolean.TRUE, 85L);
            propSummary1.put(propertyName, propDetails1);
        }

        AttrConfigCategoryOverview<Boolean> attrConfig2 = new AttrConfigCategoryOverview<>();
        result.put(Category.GROWTH_TRENDS.getName(), attrConfig2);
        attrConfig2.setLimit(500L);
        attrConfig2.setTotalAttrs(9L);
        Map<String, Map<Boolean, Long>> propSummary2 = new HashMap<>();
        attrConfig2.setPropSummary(propSummary2);
        for (String propertyName : propertyNames) {
            Map<Boolean, Long> propDetails2 = new HashMap<>();
            propDetails2.put(Boolean.FALSE, 9L);
            propSummary2.put(propertyName, propDetails2);
        }

        AttrConfigCategoryOverview<Boolean> attrConfig3 = new AttrConfigCategoryOverview<>();
        result.put(Category.INTENT.getName(), attrConfig3);
        attrConfig3.setLimit(500L);
        attrConfig3.setTotalAttrs(10960L);
        Map<String, Map<Boolean, Long>> propSummary3 = new HashMap<>();
        attrConfig3.setPropSummary(propSummary3);
        for (String propertyName : propertyNames) {
            Map<Boolean, Long> propDetails3 = new HashMap<>();
            propDetails3.put(Boolean.FALSE, 7368L);
            propDetails3.put(Boolean.TRUE, 3592L);
            propSummary3.put(propertyName, propDetails3);
        }

        AttrConfigCategoryOverview<Boolean> attrConfig4 = new AttrConfigCategoryOverview<>();
        result.put(Category.LEAD_INFORMATION.getName(), attrConfig4);
        attrConfig4.setLimit(500L);
        attrConfig4.setTotalAttrs(1L);
        Map<String, Map<Boolean, Long>> propSummary4 = new HashMap<>();
        attrConfig4.setPropSummary(propSummary4);
        for (String propertyName : propertyNames) {
            Map<Boolean, Long> propDetails4 = new HashMap<>();
            propDetails4.put(Boolean.FALSE, 1L);
            propSummary4.put(propertyName, propDetails4);
        }

        AttrConfigCategoryOverview<Boolean> attrConfig5 = new AttrConfigCategoryOverview<>();
        result.put(Category.ACCOUNT_INFORMATION.getName(), attrConfig5);
        attrConfig5.setLimit(500L);
        attrConfig5.setTotalAttrs(1L);
        Map<String, Map<Boolean, Long>> propSummary5 = new HashMap<>();
        attrConfig5.setPropSummary(propSummary5);
        for (String propertyName : propertyNames) {
            Map<Boolean, Long> propDetails5 = new HashMap<>();
            propDetails5.put(Boolean.FALSE, 1L);
            propSummary5.put(propertyName, propDetails5);
        }

        AttrConfigCategoryOverview<Boolean> attrConfig6 = new AttrConfigCategoryOverview<>();
        result.put(Category.ONLINE_PRESENCE.getName(), attrConfig6);
        attrConfig6.setLimit(500L);
        attrConfig6.setTotalAttrs(0L);
        Map<String, Map<Boolean, Long>> propSummary6 = new HashMap<>();
        attrConfig6.setPropSummary(propSummary6);
        for (String propertyName : propertyNames) {
            Map<Boolean, Long> propDetails6 = new HashMap<>();
            propSummary6.put(propertyName, propDetails6);
        }
        return result;
    }

    public static Map<String, AttrConfigCategoryOverview<?>> generatePremiumCategoryAttrConfigActivationOverview() {
        Map<String, AttrConfigCategoryOverview<?>> map = new HashMap<>();

        AttrConfigCategoryOverview<AttrState> intentCategoryAttrConfigOverview = new AttrConfigCategoryOverview<>();
        map.put(Category.INTENT.getName(), intentCategoryAttrConfigOverview);
        intentCategoryAttrConfigOverview.setLimit(intentLimit);
        intentCategoryAttrConfigOverview.setTotalAttrs(totalIntentAttrs);
        Map<String, Map<AttrState, Long>> propSummary = new HashMap<>();
        intentCategoryAttrConfigOverview.setPropSummary(propSummary);
        Map<AttrState, Long> valueCountMap = new HashMap<>();
        valueCountMap.put(AttrState.Active, activeForIntent);
        valueCountMap.put(AttrState.Inactive, inactiveForIntent);
        valueCountMap.put(AttrState.Deprecated, 0L);
        propSummary.put(ColumnMetadataKey.State, valueCountMap);

        AttrConfigCategoryOverview<AttrState> tpCategoryAttrConfigOverview = new AttrConfigCategoryOverview<>();
        map.put(Category.TECHNOLOGY_PROFILE.getName(), tpCategoryAttrConfigOverview);
        tpCategoryAttrConfigOverview.setLimit(tpLimit);
        tpCategoryAttrConfigOverview.setTotalAttrs(totalTpAttrs);
        propSummary = new HashMap<>();
        tpCategoryAttrConfigOverview.setPropSummary(propSummary);
        valueCountMap = new HashMap<>();
        valueCountMap.put(AttrState.Active, activeForTp);
        valueCountMap.put(AttrState.Inactive, inactiveForTp);
        propSummary.put(ColumnMetadataKey.State, valueCountMap);

        AttrConfigCategoryOverview<AttrState> accountCategoryAttrConfigOverview = new AttrConfigCategoryOverview<>();
        map.put(Category.ACCOUNT_ATTRIBUTES.getName(), accountCategoryAttrConfigOverview);
        accountCategoryAttrConfigOverview.setLimit(accountLimit);
        accountCategoryAttrConfigOverview.setTotalAttrs(totalAccountAttrs);
        propSummary = new HashMap<>();
        accountCategoryAttrConfigOverview.setPropSummary(propSummary);
        valueCountMap = new HashMap<>();
        valueCountMap.put(AttrState.Active, activeForAccount);
        valueCountMap.put(AttrState.Inactive, inactiveForAccount);
        propSummary.put(ColumnMetadataKey.State, valueCountMap);

        AttrConfigCategoryOverview<AttrState> contactCategoryAttrConfigOverview = new AttrConfigCategoryOverview<>();
        map.put(Category.CONTACT_ATTRIBUTES.getName(), contactCategoryAttrConfigOverview);
        contactCategoryAttrConfigOverview.setLimit(contactLimit);
        contactCategoryAttrConfigOverview.setTotalAttrs(totalContactAttrs);
        propSummary = new HashMap<>();
        contactCategoryAttrConfigOverview.setPropSummary(propSummary);
        valueCountMap = new HashMap<>();
        valueCountMap.put(AttrState.Active, activeForContact);
        valueCountMap.put(AttrState.Inactive, inactiveForContact);
        propSummary.put(ColumnMetadataKey.State, valueCountMap);

        AttrConfigCategoryOverview<AttrState> websiteKeywordCategoryAttrConfigOverview = new AttrConfigCategoryOverview<>();
        map.put(Category.WEBSITE_KEYWORDS.getName(), websiteKeywordCategoryAttrConfigOverview);
        websiteKeywordCategoryAttrConfigOverview.setLimit(websiteKeywordLimit);
        websiteKeywordCategoryAttrConfigOverview.setTotalAttrs(totalWebsiteKeywordAttrs);
        propSummary = new HashMap<>();
        websiteKeywordCategoryAttrConfigOverview.setPropSummary(propSummary);
        valueCountMap = new HashMap<>();
        valueCountMap.put(AttrState.Active, activeForWebsiteKeyword);
        valueCountMap.put(AttrState.Inactive, inactiveForWebsiteKeyword);
        propSummary.put(ColumnMetadataKey.State, valueCountMap);

        log.info("map is " + map);
        return map;
    }

    public static Map<String, AttrConfigCategoryOverview<?>> generateAccountAndContactCategoryAttrConfigNameOverview() {
        Map<String, AttrConfigCategoryOverview<?>> map = new HashMap<>();

        AttrConfigCategoryOverview<AttrState> accountCategoryAttrConfigOverview = new AttrConfigCategoryOverview<>();
        map.put(Category.ACCOUNT_ATTRIBUTES.getName(), accountCategoryAttrConfigOverview);
        accountCategoryAttrConfigOverview.setLimit(accountLimit);
        accountCategoryAttrConfigOverview.setTotalAttrs(totalAccountAttrs);
        Map<String, Map<AttrState, Long>> propSummary = new HashMap<>();
        accountCategoryAttrConfigOverview.setPropSummary(propSummary);
        Map<AttrState, Long> valueCountMap = new HashMap<>();
        valueCountMap.put(AttrState.Active, activeForAccount);
        valueCountMap.put(AttrState.Inactive, inactiveForAccount);
        propSummary.put(ColumnMetadataKey.State, valueCountMap);

        AttrConfigCategoryOverview<AttrState> contactCategoryAttrConfigOverview = new AttrConfigCategoryOverview<>();
        map.put(Category.CONTACT_ATTRIBUTES.getName(), contactCategoryAttrConfigOverview);
        contactCategoryAttrConfigOverview.setLimit(contactLimit);
        contactCategoryAttrConfigOverview.setTotalAttrs(totalContactAttrs);
        propSummary = new HashMap<>();
        contactCategoryAttrConfigOverview.setPropSummary(propSummary);
        valueCountMap = new HashMap<>();
        valueCountMap.put(AttrState.Active, activeForContact);
        valueCountMap.put(AttrState.Inactive, inactiveForContact);
        propSummary.put(ColumnMetadataKey.State, valueCountMap);

        log.info("map is " + map);
        return map;
    }

    public static AttrConfigRequest generateHappyAttrConfigRequest() {
        AttrConfigRequest attrConfigRequest = new AttrConfigRequest();
        List<AttrValidation> validations = new ArrayList<>();
        ValidationDetails details = new ValidationDetails();
        attrConfigRequest.setDetails(details);
        details.setValidations(validations);
        return attrConfigRequest;
    }

    public static AttrConfigRequest generateValidationErrorAttrConfigRequest() {
        AttrConfigRequest attrConfigRequest = generateHappyAttrConfigRequest();
        String subcategory1 = "sub1";
        String attrName1 = deselect[0];
        String subcategory2 = "sub2";
        String attrName2 = deselect[1];

        ValidationDetails details = attrConfigRequest.getDetails();
        List<AttrValidation> validations = details.getValidations();
        AttrValidation validation1 = new AttrValidation();
        validation1.setAttrName(attrName1);
        validation1.setSubcategory(subcategory1);
        ValidationErrors validationErrors1 = new ValidationErrors();
        validation1.setValidationErrors(validationErrors1);
        validations.add(validation1);
        validationErrors1.setErrors(ImmutableMap.<ValidationErrors.Type, List<String>> builder()
                .put(ValidationErrors.Type.EXCEED_SYSTEM_LIMIT, Arrays.asList("You are trying to enable 500 Account " +
                        "attributes, Please not choose more than the limit 400")) //
                .put(ValidationErrors.Type.EXCEED_USAGE_LIMIT, Arrays.asList("You are trying to enable 500 Company " +
                        "Profile attributes, Please not choose more than the limit 400")) //
                .put(ValidationErrors.Type.INVALID_ACTIVATION, Arrays.asList("User cannot change deprecated attribute" +
                        " to active.")) //
                .put(ValidationErrors.Type.INVALID_USAGE_CHANGE, Arrays.asList("Usage change is not allowed(Usage " +
                        "group Model ,"
                        + "Attribute type Account)")) //
                .build());

        AttrValidation validation2 = new AttrValidation();
        validation2.setAttrName(attrName2);
        validation2.setSubcategory(subcategory2);
        ValidationErrors validationErrors2 = new ValidationErrors();
        validation2.setValidationErrors(validationErrors2);
        validations.add(validation2);
        validationErrors2.setErrors(ImmutableMap.<ValidationErrors.Type, List<String>> builder()
                .put(ValidationErrors.Type.EXCEED_DATA_LICENSE, Arrays.asList("You are trying to enable 100 Intent " +
                        "attributes, Please not choose more than the limit 80")) //
                .put(ValidationErrors.Type.INVALID_PROP_CHANGE, Arrays.asList("Property State does not allow " +
                        "customization.")) //
                .put(ValidationErrors.Type.INVALID_ACTIVATION, Arrays.asList("User cannot change deprecated attribute" +
                        " to active.")) //
                .put(ValidationErrors.Type.INVALID_USAGE_CHANGE, Arrays.asList("Usage change is not allowed(Usage " +
                        "group Model ,"
                        + "Attribute type Account)")) //
                .build());

        // add one validation that only contains warning, the request still will be regarded as error
        AttrValidation validation3 = new AttrValidation();
        validation3.setAttrName(attrName1);
        validation3.setSubcategory(subcategory1);
        ImpactWarnings impactWarnings = new ImpactWarnings();
        validation3.setImpactWarnings(impactWarnings);
        validations.add(validation3);
        impactWarnings.setWarnings(ImmutableMap.<ImpactWarnings.Type, List<String>>builder().put(ImpactWarnings.Type.IMPACTED_SEGMENTS, Arrays.asList(
                "seg1", "seg2", "seg3")).build());
        return attrConfigRequest;
    }

    public static AttrConfigRequest generateAttrLevelAttrConfigRequest(boolean updateUsage) {
        AttrConfigRequest attrConfigRequest = generateHappyAttrConfigRequest();
        String subcategory = "sub1";
        String attrName = deselect[0];

        List<AttrConfig> attrConfigs = new ArrayList<>();
        attrConfigRequest.setAttrConfigs(attrConfigs);
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName(attrName);
        attrConfig.setEntity(BusinessEntity.Account);
        Map<String, AttrConfigProp<?>> attrProps = new HashMap<>();
        AttrConfigProp<String> subcategoryProp = new AttrConfigProp<String>();
        subcategoryProp.setSystemValue(subcategory);
        attrProps.put(ColumnMetadataKey.Subcategory, subcategoryProp);
        attrConfig.setAttrProps(attrProps);

        ValidationDetails details = attrConfigRequest.getDetails();
        List<AttrValidation> validations = details.getValidations();
        AttrValidation validation = new AttrValidation();
        validation.setAttrName(attrName);
        validation.setSubcategory(subcategory);
        ImpactWarnings impactWarnings = new ImpactWarnings();
        Map<ImpactWarnings.Type, List<String>> warnings = new HashMap<>();
        if (updateUsage) {
            warnings.put(ImpactWarnings.Type.IMPACTED_SEGMENTS, Arrays.asList("seg1", "seg2", "seg3"));
            warnings.put(ImpactWarnings.Type.IMPACTED_RATING_ENGINES, Arrays.asList("re1", "re2", "re3"));
            warnings.put(ImpactWarnings.Type.IMPACTED_RATING_MODELS, Arrays.asList("rm1", "rm2", "rm3"));
        } else {
            warnings.put(ImpactWarnings.Type.USAGE_ENABLED,
                    Arrays.asList(ColumnSelection.Predefined.Segment.getName(),
                            ColumnSelection.Predefined.CompanyProfile.getName(),
                            ColumnSelection.Predefined.Enrichment.getName()));
        }
        impactWarnings.setWarnings(warnings);
        validation.setImpactWarnings(impactWarnings);
        validations.add(validation);
        return attrConfigRequest;
    }

    public static AttrConfigRequest generateSubcategoryLevelAttrConfigRequest(boolean updateUsage) {
        AttrConfigRequest attrConfigRequest = generateAttrLevelAttrConfigRequest(updateUsage);

        String subcategory = "sub1";
        String attrName = deselect[1];

        AttrConfig attrConfig = new AttrConfig();
        attrConfigRequest.getAttrConfigs().add(attrConfig);
        attrConfig.setAttrName(attrName);
        attrConfig.setEntity(BusinessEntity.Account);
        Map<String, AttrConfigProp<?>> attrProps = new HashMap<>();
        AttrConfigProp<String> subcategoryProp = new AttrConfigProp<String>();
        subcategoryProp.setSystemValue(subcategory);
        attrProps.put(ColumnMetadataKey.Subcategory, subcategoryProp);
        attrConfig.setAttrProps(attrProps);

        AttrValidation validation = new AttrValidation();
        validation.setAttrName(attrName);
        validation.setSubcategory(subcategory);
        attrConfigRequest.getDetails().getValidations().add(validation);
        return attrConfigRequest;
    }

    public static AttrConfigRequest generateCategoryLevelAttrConfigRequest(boolean updateUsage) {
        AttrConfigRequest attrConfigRequest = generateSubcategoryLevelAttrConfigRequest(updateUsage);

        String subcategory = "sub2";
        String attrName = deselect[2];

        AttrConfig attrConfig = new AttrConfig();
        attrConfigRequest.getAttrConfigs().add(attrConfig);
        attrConfig.setAttrName(attrName);
        attrConfig.setEntity(BusinessEntity.Account);
        Map<String, AttrConfigProp<?>> attrProps = new HashMap<>();
        AttrConfigProp<String> subcategoryProp = new AttrConfigProp<String>();
        subcategoryProp.setSystemValue(subcategory);
        attrProps.put(ColumnMetadataKey.Subcategory, subcategoryProp);
        attrConfig.setAttrProps(attrProps);

        AttrValidation validation = new AttrValidation();
        validation.setAttrName(attrName);
        validation.setSubcategory(subcategory);
        attrConfigRequest.getDetails().getValidations().add(validation);
        return attrConfigRequest;
    }

}
