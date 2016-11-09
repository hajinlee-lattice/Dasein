package com.latticeengines.pls.service;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import com.latticeengines.domain.exposed.ulysses.Campaign;

public interface CampaignService {

    List<Campaign> findAll();

    Campaign createCampaignFromModels(String campaignName, String segmentName, List<String> modelIds,
            HttpServletRequest request);

    Campaign createCampaignFromTable(String campaignName, String segmentName, String tableName,
            HttpServletRequest request);

    Campaign findCampaignByName(String campaignName);

    void deleteCampaignByName(String campaignName);

}
