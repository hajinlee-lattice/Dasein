package com.latticeengines.apps.core.service.impl;

import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceapps.core.AttrType;

public class AttrConfigTestUtils {

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

}
