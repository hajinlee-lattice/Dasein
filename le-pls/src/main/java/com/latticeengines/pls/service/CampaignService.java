package com.latticeengines.pls.service;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import com.latticeengines.domain.exposed.ulysses.Campaign;

public interface CampaignService {

    List<Campaign> findAll();
    
    Campaign createCampaign(String campaignName, String description, HttpServletRequest request);
    
    Campaign createCampaignFromModels(String campaignName, String description, String segmentName, List<String> modelIds,
            HttpServletRequest request);

    Campaign createCampaignFromTable(String campaignName, String description, String segmentName, String tableName,
            HttpServletRequest request);

    Campaign findCampaignByName(String campaignName);

    void deleteCampaignByName(String campaignName);

}
