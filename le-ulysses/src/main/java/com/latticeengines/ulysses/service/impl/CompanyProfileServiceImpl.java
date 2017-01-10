package com.latticeengines.ulysses.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.ulysses.Campaign;
import com.latticeengines.domain.exposed.ulysses.CampaignType;
import com.latticeengines.domain.exposed.ulysses.CompanyProfile;
import com.latticeengines.domain.exposed.ulysses.Insight;
import com.latticeengines.domain.exposed.ulysses.InsightSection;
import com.latticeengines.domain.exposed.ulysses.InsightSourceType;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.ulysses.entitymgr.CampaignEntityMgr;
import com.latticeengines.ulysses.service.CompanyProfileService;

@Component("companyProfileService")
public class CompanyProfileServiceImpl implements CompanyProfileService {

    @Autowired
    private FabricMessageService messageService;

    @Autowired
    private FabricDataService dataService;

    @Autowired
    private MatchProxy matchProxy;

    @Autowired
    private CampaignEntityMgr campaignEntityMgr;

    @Override
    public void setupCampaignForCompanyProfile(CustomerSpace customerSpace) {
        Campaign profile = new Campaign();
        profile.setName("Company Profile Campaign");
        profile.setCampaignType(CampaignType.PROFILE);
        profile.setTenant(new Tenant(customerSpace.toString()));

        Insight insight = new Insight();
        insight.setName("Profile Insight");

        InsightSection insightSection = new InsightSection();
        insightSection.setDescription("Insight section for company profile");
        insightSection.setTip("Insight section tip for company profile");
        insightSection.setHeadline("Insight section headline for company profile");
        insightSection.setInsightSourceType(InsightSourceType.EXTERNAL);

        profile.setInsights(Arrays.asList(new Insight[] { insight }));
        insight.setInsightSections(Arrays.asList(new InsightSection[] { insightSection }));

        campaignEntityMgr.create(profile);
    }

    @Override
    public Campaign getProfileCampaign(CustomerSpace customerSpace) {
        return null;
    }

    @Override
    public CompanyProfile getProfile(CustomerSpace customerSpace, Map<MatchKey, String> matchRequest) {
        MatchInput matchInput = new MatchInput();

        List<String> fields = new ArrayList<>();
        List<List<Object>> data = new ArrayList<>();
        for (Map.Entry<MatchKey, String> entry : matchRequest.entrySet()) {
            List<Object> datum = new ArrayList<>();
            fields.add(entry.getKey().name());
            datum.add(entry.getValue());
            data.add(datum);
        }

        ColumnSelection selection = getColumnSelection(customerSpace);

        Tenant tenant = new Tenant(customerSpace.toString());
        matchInput.setTenant(tenant);
        matchInput.setFields(fields);
        matchInput.setData(data);
        matchInput.setDataCloudVersion("2.0.0");
        matchInput.setCustomSelection(selection);

        MatchOutput matchOutput = matchProxy.matchRealTime(matchInput);

        return createCompanyProfile(selection, matchOutput);
    }

    private CompanyProfile createCompanyProfile(ColumnSelection selection, MatchOutput matchOutput) {
        CompanyProfile profile = new CompanyProfile();
        List<Object> outputRecords = matchOutput.getResult().get(0).getOutput();
        for (int i = 0; i < outputRecords.size(); i++) {
            profile.attributes.put(selection.getColumnIds().get(i), String.valueOf(outputRecords.get(i)));
        }
        return profile;
    }

    private ColumnSelection getColumnSelection(CustomerSpace customerSpace) {
        ColumnSelection selection = new ColumnSelection();
        List<AccountMasterColumn> columns = new ArrayList<>();

        Campaign profile = campaignEntityMgr.findByKey(customerSpace.toString() + "|PROFILE");
        InsightSection section = profile.getInsights().get(0).getInsightSections().get(0);

        for (String amColumnId : section.getAttributes()) {
            AccountMasterColumn column = new AccountMasterColumn();
            column.setAmColumnId(amColumnId);
            columns.add(column);
        }

        selection.createAccountMasterColumnSelection(columns);

        return selection;
    }

}
