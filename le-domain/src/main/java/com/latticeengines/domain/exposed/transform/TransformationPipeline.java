package com.latticeengines.domain.exposed.transform;

import java.util.LinkedHashMap;
import java.util.Map;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.sun.tools.classfile.Annotation.element_value;

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
    
    public static TransformDefinition stdVisidbEmailIsInvalid = new TransformDefinition("StdVisidbDsEmailIsInvalid",
            "EmailIsInvalid", FieldType.BOOLEAN, new LinkedHashMap<String, Object>());
            
    public static TransformDefinition stdVisidbEmailLength = new TransformDefinition("StdVisidbDsEmailLength",
            "EmailLength", FieldType.INTEGER, new LinkedHashMap<String, Object>());
    
    public static TransformDefinition stdVisidbEmailPrefixLength = new TransformDefinition("StdVisidbDsEmailPrefixlength",
            "EmailPrefixLength", FieldType.INTEGER, new LinkedHashMap<String, Object>());
    
    public static TransformDefinition stdVisidbNameLength = new TransformDefinition("StdVisidbDsNamelength",
            "NameLength", FieldType.INTEGER, new LinkedHashMap<String, Object>());
    
    public static TransformDefinition stdVisidbTitleChannel = new TransformDefinition("StdVisidbDsTitleChannel",
            "TitleChannel", FieldType.STRING, new LinkedHashMap<String, Object>());
    
    public static TransformDefinition stdVisidbTitleFunction = new TransformDefinition("StdVisidbDsTitleFunction",
            "TitleFunction", FieldType.STRING, new LinkedHashMap<String, Object>());
    
    public static TransformDefinition stdVisidbTitleLevelCategorical = new TransformDefinition("StdVisidbDsTitleLevelCategorical",
            "TitleLevelCategorical", FieldType.STRING, new LinkedHashMap<String, Object>());
    
    public static TransformDefinition stdVisidbTitleRole = new TransformDefinition("StdVisidbDsTitleRole",
            "TitleRole", FieldType.STRING, new LinkedHashMap<String, Object>());
    
    public static TransformDefinition stdVisidbTitleScope = new TransformDefinition("StdVisidbDsTitleScope",
            "TitleScope", FieldType.STRING, new LinkedHashMap<String, Object>());
    
    public static TransformDefinition stdVisidbStateIsCanadianProvince = new TransformDefinition("StdVisidbDsStateIsCanadianProvince",
            "StateIsCanadianProvince", FieldType.BOOLEAN, new LinkedHashMap<String, Object>());
    
    public static TransformDefinition stdVisidbStateIsInFarWest = new TransformDefinition("StdVisidbDsStateIsInFarWest",
            "StateIsInFarWest", FieldType.BOOLEAN, new LinkedHashMap<String, Object>());
    
    public static TransformDefinition stdVisidbStateIsInGreatLakes = new TransformDefinition("StdVisidbDsStateIsInGreatLakes",
            "StateIsInGreatLakes", FieldType.BOOLEAN, new LinkedHashMap<String, Object>());
    
    public static TransformDefinition stdVisidbStateIsInMidAtlantic = new TransformDefinition("StdVisidbDsStateIsInMidAtlantic",
            "StateIsInMidAtlantic", FieldType.BOOLEAN, new LinkedHashMap<String, Object>());
    
    public static TransformDefinition stdVisidbStateIsInNewEngland = new TransformDefinition("StdVisidbDsStateIsInNewEngland",
            "StateIsInNewEngland", FieldType.BOOLEAN, new LinkedHashMap<String, Object>());
    
    public static TransformDefinition stdVisidbStateIsInPlains = new TransformDefinition("StdVisidbDsStateIsInPlains",
            "StateIsInPlains", FieldType.BOOLEAN, new LinkedHashMap<String, Object>());
    
    public static TransformDefinition stdVisidbStateIsInRockyMountain = new TransformDefinition("StdVisidbDsStateIsInRockyMountains",
            "StateIsInRockyMountains", FieldType.BOOLEAN, new LinkedHashMap<String, Object>());
    
    public static TransformDefinition stdVisidbStateIsInSouthEast = new TransformDefinition("StdVisidbDsStateIsInSouthEast",
            "StateIsInSouthEast", FieldType.BOOLEAN, new LinkedHashMap<String, Object>());
    
    public static TransformDefinition stdVisidbStateIsInSouthWest = new TransformDefinition("StdVisidbDsStateIsInSouthWest",
            "StateIsInSouthWest", FieldType.BOOLEAN, new LinkedHashMap<String, Object>());
    
    
    static {
        stdVisidbDsCompanynameEntropy.arguments.put("column", InterfaceName.CompanyName.name());

        stdLengthTitle.arguments.put("column", InterfaceName.Title.name());
        stdLengthTitle.outputDisplayName = "Length of Title";

        stdLengthCompanyName.arguments.put("column", InterfaceName.CompanyName.name());
        stdLengthCompanyName.outputDisplayName = "Length of Company Name";

        // need to set stdLengthDomain arguments
        stdLengthDomain.outputDisplayName = "Length of Domain Name";

        stdVisidbDsPdAlexaRelatedlinksCount.arguments.put("column", "AlexaRelatedLinks");

        stdPhoneEntropy.arguments.put("column", InterfaceName.PhoneNumber.name());
        stdPhoneEntropy.outputDisplayName = "Entropy of Phone Number";

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
                
                .put(stdVisidbTitleChannel.name + "_" + stdVisidbTitleChannel.output, stdVisidbTitleChannel) 
                
                .put(stdVisidbTitleFunction.name + "_" + stdVisidbTitleFunction.output, stdVisidbTitleFunction)
                
                .put(stdVisidbTitleLevelCategorical.name + "_" + stdVisidbTitleLevelCategorical.output, stdVisidbTitleLevelCategorical)
                
                .put(stdVisidbTitleRole.name + "_" + stdVisidbTitleRole.output, stdVisidbTitleRole)
                
                .put(stdVisidbTitleScope.name + "_" + stdVisidbTitleScope.output, stdVisidbTitleScope)
                
                .put(stdVisidbNameLength.name + "_" + stdVisidbNameLength.output, stdVisidbNameLength)                                
                
                .put(stdVisidbEmailIsInvalid.name + "_" + stdVisidbEmailIsInvalid.output, stdVisidbEmailIsInvalid)
                
                .put(stdVisidbEmailLength.name + "_" + stdVisidbEmailLength.output, stdVisidbEmailLength)
                
                .put(stdVisidbEmailPrefixLength.name + "_" + stdVisidbEmailPrefixLength.output, stdVisidbEmailPrefixLength)
                
                .put(stdVisidbStateIsCanadianProvince.name + "_" + stdVisidbStateIsCanadianProvince.output, stdVisidbStateIsCanadianProvince)
                
                .put(stdVisidbStateIsInFarWest.name + "_" + stdVisidbStateIsInFarWest.output, stdVisidbStateIsInFarWest)
                
                .put(stdVisidbStateIsInGreatLakes.name + "_" + stdVisidbStateIsInGreatLakes.output, stdVisidbStateIsInGreatLakes)
                
                .put(stdVisidbStateIsInMidAtlantic.name + "_" + stdVisidbStateIsInMidAtlantic.output, stdVisidbStateIsInMidAtlantic)
                
                .put(stdVisidbStateIsInNewEngland.name + "_" + stdVisidbStateIsInNewEngland.output, stdVisidbStateIsInNewEngland)
                
                .put(stdVisidbStateIsInPlains.name + "_" + stdVisidbStateIsInPlains.output, stdVisidbStateIsInPlains)
                
                .put(stdVisidbStateIsInRockyMountain.name + "_" + stdVisidbStateIsInRockyMountain.output, stdVisidbStateIsInRockyMountain)
                
                .put(stdVisidbStateIsInSouthEast.name + "_" + stdVisidbStateIsInSouthEast.output, stdVisidbStateIsInSouthEast)
                
                .put(stdVisidbStateIsInSouthWest.name + "_" + stdVisidbStateIsInSouthWest.output, stdVisidbStateIsInSouthWest)
                
                .build();
    }
}
