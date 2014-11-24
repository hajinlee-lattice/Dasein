package com.latticeengines.skald;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.skald.model.DataComposition;
import com.latticeengines.skald.model.FieldInterpretation;
import com.latticeengines.skald.model.FieldSchema;
import com.latticeengines.skald.model.FieldSource;
import com.latticeengines.skald.model.FieldType;
import com.latticeengines.skald.model.ScoreDerivation;
import com.latticeengines.skald.model.TransformDefinition;

// Retrieves and caches active model structures, transform definitions, and score derivations.
@Service
public class CombinationRetriever {
    public List<CombinationElement> getCombination(CustomerSpace spaceID, String combination) {
        // TODO Add a caching layer.

        DataComposition data = new DataComposition();

        data.transforms = new ArrayList<TransformDefinition>();
        Map<String, Object> arguments = new HashMap<String, Object>();
        arguments.put("value", 10.0);
        TransformDefinition transform = new TransformDefinition("echo", "victory_field", FieldType.Float, arguments);
        data.transforms.add(transform);

        // TODO Retrieve the other structures.
        CombinationElement element = new CombinationElement();
        element.derivation = new ScoreDerivation();
        element.data = data;
        element.data.fields = new HashMap<String, FieldSchema>();

        element.data.fields.put("BankruptcyFiled", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BusinessAnnualSalesAbs", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BusinessECommerceSite", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BusinessEstablishedYear", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BusinessEstimatedAnnualSales_k", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BusinessEstimatedEmployees", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BusinessFirmographicsParentEmployees", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("BusinessFirmographicsParentRevenue", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("BusinessRetirementParticipants", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BusinessSocialPresence", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BusinessUrlNumPages", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BusinessVCFunded", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("DerogatoryIndicator", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("ExperianCreditRating", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("FundingAgency", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("FundingAmount", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("FundingAwardAmount", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("FundingFinanceRound", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("FundingReceived", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("FundingStage", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("Intelliscore", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("JobsRecentJobs", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("JobsTrendString", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("ModelAction", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("PercentileModel", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("UCCFilings", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("UCCFilingsPresent", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("Years_in_Business_Code", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("Non_Profit_Indicator", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("PD_DA_AwardCategory", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("PD_DA_JobTitle", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("PD_DA_LastSocialActivity_Units", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("PD_DA_MonthsPatentGranted", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("PD_DA_MonthsSinceFundAwardDate", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("PD_DA_PrimarySIC1", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("AnnualRevenue", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("NumberOfEmployees", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("Alexa_MonthsSinceOnline", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("Alexa_Rank", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("Alexa_ReachPerMillion", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("Alexa_ViewsPerMillion", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("Alexa_ViewsPerUser", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_TechTags_Cnt", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_TotalTech_Cnt", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_ads", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_analytics", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_cdn", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_cdns", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_cms", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_docinfo", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_encoding", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_feeds", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_framework", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_hosting", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_javascript", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_mapping", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_media", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_mx", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_ns", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_parked", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_payment", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_seo_headers", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_seo_meta", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_seo_title", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_Server", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_shop", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_ssl", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_Web_Master", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_Web_Server", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("BW_widgets", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("Activity_ClickLink_cnt", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("Activity_VisitWeb_cnt", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("Activity_InterestingMoment_cnt", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("Activity_OpenEmail_cnt", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("Activity_EmailBncedSft_cnt", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("Activity_FillOutForm_cnt", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("Activity_UnsubscrbEmail_cnt", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("Activity_ClickEmail_cnt", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_CloudService_One", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_CloudService_Two", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_CommTech_One", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_CommTech_Two", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_CRM_One", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_CRM_Two", new FieldSchema(FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_DataCenterSolutions_One", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_DataCenterSolutions_Two", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_EnterpriseApplications_One", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_EnterpriseApplications_Two", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_EnterpriseContent_One", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_EnterpriseContent_Two", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_HardwareBasic_One", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_HardwareBasic_Two", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_ITGovernance_One", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_ITGovernance_Two", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_MarketingPerfMgmt_One", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_MarketingPerfMgmt_Two", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_NetworkComputing_One", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_NetworkComputing_Two", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_ProductivitySltns_One", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_ProductivitySltns_Two", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_ProjectMgnt_One", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_ProjectMgnt_Two", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_SoftwareBasic_One", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_SoftwareBasic_Two", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_VerticalMarkets_One", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_VerticalMarkets_Two", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_WebOrntdArch_One", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));
        element.data.fields.put("CloudTechnologies_WebOrntdArch_Two", new FieldSchema(FieldSource.Request,
                FieldType.Float, FieldInterpretation.Feature));

        List<CombinationElement> result = new ArrayList<CombinationElement>();
        result.add(element);
        return result;
    }
}
