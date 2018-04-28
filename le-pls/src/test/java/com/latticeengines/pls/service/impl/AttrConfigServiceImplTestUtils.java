package com.latticeengines.pls.service.impl;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceapps.core.AttrSubType;
import com.latticeengines.domain.exposed.serviceapps.core.AttrType;

public class AttrConfigServiceImplTestUtils {

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
        prop7.setAllowCustomization(true);
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

}
