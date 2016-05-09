package com.latticeengines.domain.exposed.transform;

import java.util.LinkedHashMap;
import java.util.Map;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;

public class TransformationPipeline {

    public final static Map<String, TransformDefinition> definitions;

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

    static {
        stdVisidbDsCompanynameEntropy.arguments.put("column", InterfaceName.CompanyName.name());
        stdLengthTitle.arguments.put("column", InterfaceName.Title.name());
        stdLengthCompanyName.arguments.put("column", InterfaceName.CompanyName.name());
        // need to set stdLengthDomain

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

        definitions = ImmutableMap
                .<String, TransformDefinition> builder()
                //
                .put(stdVisidbDsCompanynameEntropy.name + "_" + stdVisidbDsCompanynameEntropy.output,
                        stdVisidbDsCompanynameEntropy) //

                .put(stdLengthTitle.name + "_" + stdLengthTitle.output, stdLengthTitle) //

                .put(stdLengthCompanyName.name + "_" + stdLengthCompanyName.output, stdLengthCompanyName) //

                .put(stdLengthDomain.name + "_" + stdLengthDomain.output, stdLengthDomain) //

                .put(stdVisidbDsPdAlexaRelatedlinksCount.name + "_" + stdVisidbDsPdAlexaRelatedlinksCount.output,
                        stdVisidbDsPdAlexaRelatedlinksCount) //

                .put(stdPhoneEntropy.name + "_" + stdPhoneEntropy.output, stdPhoneEntropy) //

                .put(stdVisidbAlexaMonthssinceonline.name + "_" + stdVisidbAlexaMonthssinceonline.output,
                        stdVisidbAlexaMonthssinceonline) //

                .put(stdVisidbDsPdModelactionOrdered.name + "_" + stdVisidbDsPdModelactionOrdered.output,
                        stdVisidbDsPdModelactionOrdered) //

                .put(stdVisidbDsSpamindicator.name + "_" + stdVisidbDsSpamindicator.output, stdVisidbDsSpamindicator) //

                .put(stdVisidbDsTitleLevel.name + "_" + stdVisidbDsTitleLevel.output, stdVisidbDsTitleLevel) //

                .put(stdVisidbDsTitleIstechrelated.name + "_" + stdVisidbDsTitleIstechrelated.output,
                        stdVisidbDsTitleIstechrelated) //

                .put(stdVisidbDsPdJobstrendstringOrdered.name + "_" + stdVisidbDsPdJobstrendstringOrdered.output,
                        stdVisidbDsPdJobstrendstringOrdered) //

                .put(stdVisidbDsPdFundingstageOrdered.name + "_" + stdVisidbDsPdFundingstageOrdered.output,
                        stdVisidbDsPdFundingstageOrdered) //

                .put(stdVisidbDsTitleIsacademic.name + "_" + stdVisidbDsTitleIsacademic.output,
                        stdVisidbDsTitleIsacademic) //

                .put(stdVisidbDsFirstnameSameasLastname.name + "_" + stdVisidbDsFirstnameSameasLastname.output,
                        stdVisidbDsFirstnameSameasLastname) //

                .put(stdVisidbDsIndustryGroup.name + "_" + stdVisidbDsIndustryGroup.output, stdVisidbDsIndustryGroup) //
                .build();
    }
}
