package com.latticeengines.domain.exposed.transform;

import java.util.LinkedHashMap;
import java.util.Set;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;

public class TransformationPipeline {

    public static TransformDefinition stdVisidbDsCompanynameEntropy = new TransformDefinition(
            "StdVisidbDsCompanynameEntropy", "CompanyName_Entropy", FieldType.FLOAT,
            new LinkedHashMap<String, Object>());

    public static TransformDefinition stdLengthTitle = new TransformDefinition("StdLength", "Title_Length",
            FieldType.INTEGER, new LinkedHashMap<String, Object>());

    public static TransformDefinition stdLengthCompanyName = new TransformDefinition("StdLength", "CompanyName_Length",
            FieldType.INTEGER, new LinkedHashMap<String, Object>());

    public static TransformDefinition stdLengthDomain = new TransformDefinition("StdLength", "Domain_Length",
            FieldType.INTEGER, new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbDsPdAlexaRelatedlinksCount = new TransformDefinition(
            "StdVisidbDsPdAlexaRelatedlinksCount", "RelatedLinks_Count", FieldType.INTEGER,
            new LinkedHashMap<String, Object>());

    public static TransformDefinition stdPhoneEntropy = new TransformDefinition("StdEntropy", "Phone_Entropy",
            FieldType.FLOAT, new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbAlexaMonthssinceonline = new TransformDefinition(
            "StdVisidbAlexaMonthssinceonline", "Website_Age_Months", FieldType.LONG,
            new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbDsPdModelactionOrdered = new TransformDefinition(
            "StdVisidbDsPdModelactionOrdered", "ModelAction1", FieldType.INTEGER, new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbDsSpamindicator = new TransformDefinition("StdVisidbDsSpamindicator",
            "SpamIndicator", FieldType.INTEGER, new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbDsTitleLevel = new TransformDefinition("StdVisidbDsTitleLevel",
            "Title_Level", FieldType.INTEGER, new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbDsTitleIstechrelated = new TransformDefinition(
            "StdVisidbDsTitleIstechrelated", "Title_IsTechRelated", FieldType.BOOLEAN,
            new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbDsPdJobstrendstringOrdered = new TransformDefinition(
            "StdVisidbDsPdJobstrendstringOrdered", "JobsTrendString1", FieldType.INTEGER,
            new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbDsPdFundingstageOrdered = new TransformDefinition(
            "StdVisidbDsPdFundingstageOrdered", "FundingStage1", FieldType.INTEGER, new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbDsTitleIsacademic = new TransformDefinition(
            "StdVisidbDsTitleIsacademic", "Title_IsAcademic", FieldType.BOOLEAN, new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbDsFirstnameSameasLastname = new TransformDefinition(
            "StdVisidbDsFirstnameSameasLastname", "FirstName_SameAs_LastName", FieldType.BOOLEAN,
            new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbDsIndustryGroup = new TransformDefinition("StdVisidbDsIndustryGroup",
            "Industry_Group", FieldType.STRING, new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbEmailIsInvalid = new TransformDefinition("StdVisidbDsEmailIsInvalid",
            "EmailIsInvalid", FieldType.BOOLEAN, new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbEmailLength = new TransformDefinition("StdVisidbDsEmailLength",
            "EmailLength", FieldType.INTEGER, new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbEmailPrefixLength = new TransformDefinition(
            "StdVisidbDsEmailPrefixlength", "EmailPrefixLength", FieldType.INTEGER, new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbNameLength = new TransformDefinition("StdVisidbDsNamelength",
            "NameLength", FieldType.INTEGER, new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbTitleChannel = new TransformDefinition("StdVisidbDsTitleChannel",
            "TitleChannel", FieldType.STRING, new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbTitleFunction = new TransformDefinition("StdVisidbDsTitleFunction",
            "TitleFunction", FieldType.STRING, new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbTitleLevelCategorical = new TransformDefinition(
            "StdVisidbDsTitleLevelCategorical", "TitleLevelCategorical", FieldType.STRING,
            new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbTitleRole = new TransformDefinition("StdVisidbDsTitleRole", "TitleRole",
            FieldType.STRING, new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbTitleScope = new TransformDefinition("StdVisidbDsTitleScope",
            "TitleScope", FieldType.STRING, new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbStateIsCanadianProvince = new TransformDefinition(
            "StdVisidbDsStateIsCanadianProvince", "StateIsCanadianProvince", FieldType.BOOLEAN,
            new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbStateIsInFarWest = new TransformDefinition(
            "StdVisidbDsStateIsInFarWest", "StateIsInFarWest", FieldType.BOOLEAN, new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbStateIsInGreatLakes = new TransformDefinition(
            "StdVisidbDsStateIsInGreatLakes", "StateIsInGreatLakes", FieldType.BOOLEAN,
            new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbStateIsInMidAtlantic = new TransformDefinition(
            "StdVisidbDsStateIsInMidAtlantic", "StateIsInMidAtlantic", FieldType.BOOLEAN,
            new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbStateIsInNewEngland = new TransformDefinition(
            "StdVisidbDsStateIsInNewEngland", "StateIsInNewEngland", FieldType.BOOLEAN,
            new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbStateIsInPlains = new TransformDefinition("StdVisidbDsStateIsInPlains",
            "StateIsInPlains", FieldType.BOOLEAN, new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbStateIsInRockyMountain = new TransformDefinition(
            "StdVisidbDsStateIsInRockyMountains", "StateIsInRockyMountains", FieldType.BOOLEAN,
            new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbStateIsInSouthEast = new TransformDefinition(
            "StdVisidbDsStateIsInSouthEast", "StateIsInSouthEast", FieldType.BOOLEAN,
            new LinkedHashMap<String, Object>());

    public static TransformDefinition stdVisidbStateIsInSouthWest = new TransformDefinition(
            "StdVisidbDsStateIsInSouthWest", "StateIsInSouthWest", FieldType.BOOLEAN,
            new LinkedHashMap<String, Object>());

    static {
        setArguments();
        setDisplayName();
    }

    public static void setArguments() {
        stdVisidbDsCompanynameEntropy.arguments.put("column", InterfaceName.CompanyName.name());

        stdLengthTitle.arguments.put("column", InterfaceName.Title.name());

        stdLengthCompanyName.arguments.put("column", InterfaceName.CompanyName.name());

        // need to set stdLengthDomain arguments

        stdVisidbDsPdAlexaRelatedlinksCount.arguments.put("column", "AlexaRelatedLinks");

        stdPhoneEntropy.arguments.put("column", InterfaceName.PhoneNumber.name());

        stdVisidbAlexaMonthssinceonline.arguments.put("column", "AlexaOnlineSince");
        stdVisidbDsPdModelactionOrdered.arguments.put("column", "ModelAction");

        stdVisidbDsSpamindicator.arguments.put("column1", InterfaceName.FirstName.name());
        stdVisidbDsSpamindicator.arguments.put("column2", InterfaceName.LastName.name());
        stdVisidbDsSpamindicator.arguments.put("column3", InterfaceName.Title.name());
        stdVisidbDsSpamindicator.arguments.put("column4", InterfaceName.PhoneNumber.name());
        stdVisidbDsSpamindicator.arguments.put("column5", InterfaceName.CompanyName.name());

        stdVisidbDsTitleLevel.arguments.put("column", InterfaceName.Title.name());
        stdVisidbDsTitleIstechrelated.arguments.put("column", InterfaceName.Title.name());
        stdVisidbDsPdJobstrendstringOrdered.arguments.put("column", "JobsTrendString");
        stdVisidbDsPdFundingstageOrdered.arguments.put("column", "FundingStage");
        stdVisidbDsTitleIsacademic.arguments.put("column", InterfaceName.Title.name());

        stdVisidbDsFirstnameSameasLastname.arguments.put("column1", InterfaceName.FirstName.name());
        stdVisidbDsFirstnameSameasLastname.arguments.put("column2", InterfaceName.LastName.name());

        stdVisidbDsIndustryGroup.arguments.put("column", InterfaceName.Industry.name());

        stdVisidbEmailIsInvalid.arguments.put("column", InterfaceName.Email.name());

        stdVisidbEmailLength.arguments.put("column", InterfaceName.Email.name());

        stdVisidbEmailPrefixLength.arguments.put("column", InterfaceName.Email.name());

        stdVisidbNameLength.arguments.put("column1", InterfaceName.FirstName.name());
        stdVisidbNameLength.arguments.put("column2", InterfaceName.LastName.name());

        stdVisidbTitleChannel.arguments.put("column", InterfaceName.Title.name());

        stdVisidbTitleFunction.arguments.put("column", InterfaceName.Title.name());
        stdVisidbTitleLevelCategorical.arguments.put("column", InterfaceName.Title.name());
        stdVisidbTitleRole.arguments.put("column", InterfaceName.Title.name());
        stdVisidbTitleScope.arguments.put("column", InterfaceName.Title.name());

        stdVisidbStateIsCanadianProvince.arguments.put("column", InterfaceName.State.name());
        stdVisidbStateIsInFarWest.arguments.put("column", InterfaceName.State.name());
        stdVisidbStateIsInGreatLakes.arguments.put("column", InterfaceName.State.name());
        stdVisidbStateIsInMidAtlantic.arguments.put("column", InterfaceName.State.name());
        stdVisidbStateIsInNewEngland.arguments.put("column", InterfaceName.State.name());
        stdVisidbStateIsInPlains.arguments.put("column", InterfaceName.State.name());
        stdVisidbStateIsInRockyMountain.arguments.put("column", InterfaceName.State.name());
        stdVisidbStateIsInSouthEast.arguments.put("column", InterfaceName.State.name());
        stdVisidbStateIsInSouthWest.arguments.put("column", InterfaceName.State.name());
    }

    public static void setDisplayName() {
        stdLengthTitle.outputDisplayName = "Length of Title";

        stdLengthCompanyName.outputDisplayName = "Length of Company Name";

        stdLengthDomain.outputDisplayName = "Length of Domain Name";

        stdPhoneEntropy.outputDisplayName = "Entropy of Phone Number";
    }

    public static Set<TransformDefinition> getTransforms(TransformationGroup group) {
        switch (group) {
        case ALL:
            return ImmutableSet.<TransformDefinition> builder()//
                    .addAll(getStandardTransforms()) //
                    .addAll(getPocTransforms()).build();
        case POC:
            return getPocTransforms();
        case STANDARD:
        default:
            return getStandardTransforms();
        }
    }

    public static Set<TransformDefinition> getStandardTransforms() {
        Set<TransformDefinition> stdTransformDefinitions = ImmutableSet.<TransformDefinition> builder()
                .add(stdVisidbDsCompanynameEntropy) //
                .add(stdLengthTitle) //
                .add(stdLengthCompanyName) //
                .add(stdLengthDomain) //
                .add(stdVisidbDsPdAlexaRelatedlinksCount) //
                .add(stdPhoneEntropy) //
                .add(stdVisidbAlexaMonthssinceonline) //
                .add(stdVisidbDsPdModelactionOrdered) //
                .add(stdVisidbDsSpamindicator) //
                .add(stdVisidbDsTitleLevel) //
                .add(stdVisidbDsTitleIstechrelated) //
                .add(stdVisidbDsPdJobstrendstringOrdered) //
                .add(stdVisidbDsPdFundingstageOrdered) //
                .add(stdVisidbDsTitleIsacademic) //
                .add(stdVisidbDsFirstnameSameasLastname) //
                .add(stdVisidbDsIndustryGroup) //
                .build();
        return stdTransformDefinitions;
    }

    public static Set<TransformDefinition> getPocTransforms() {
        Set<TransformDefinition> pocTransformDefinitions = ImmutableSet.<TransformDefinition> builder()
                .add(stdVisidbEmailIsInvalid) //
                .add(stdVisidbEmailLength) //
                .add(stdVisidbEmailPrefixLength) //
                .add(stdVisidbNameLength) //
                .add(stdVisidbTitleChannel) //
                .add(stdVisidbTitleFunction) //
                .add(stdVisidbTitleLevelCategorical) //
                .add(stdVisidbTitleRole) //
                .add(stdVisidbTitleScope) //
                .add(stdVisidbStateIsCanadianProvince) //
                .add(stdVisidbStateIsInFarWest) //
                .add(stdVisidbStateIsInGreatLakes) //
                .add(stdVisidbStateIsInMidAtlantic) //
                .add(stdVisidbStateIsInNewEngland) //
                .add(stdVisidbStateIsInPlains) //
                .add(stdVisidbStateIsInRockyMountain) //
                .add(stdVisidbStateIsInSouthEast) //
                .add(stdVisidbStateIsInSouthWest) //
                .build();
        return pocTransformDefinitions;
    }

}
