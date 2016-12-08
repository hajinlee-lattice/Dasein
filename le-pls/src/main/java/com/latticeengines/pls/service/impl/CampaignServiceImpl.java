package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.ulysses.Campaign;
import com.latticeengines.domain.exposed.ulysses.CampaignType;
import com.latticeengines.pls.entitymanager.CampaignEntityMgr;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.service.CampaignService;
import com.latticeengines.pls.service.SegmentBuilder;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.util.SecurityUtils;

@Component("campaignService")
public class CampaignServiceImpl implements CampaignService {
    
    @Autowired
    private CampaignEntityMgr campaignEntityMgr;
    
    @Autowired
    private MetadataProxy metadataProxy;
    
    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;
    
    @Autowired
    private SegmentBuilder<Table> listSegmentBuilder;
    
    @Autowired
    private SegmentBuilder<ModelSummary> modelSegmentBuilder;
    
    @Autowired
    private SessionService sessionService;
    
    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public Campaign createCampaign(String campaignName, String description, HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        tenant = tenantEntityMgr.findByTenantId(tenant.getId());
        return createCampaign(campaignName, description, tenant, new ArrayList<MetadataSegment>(), true);
    }

    @Override
    public Campaign createCampaignFromModels(String campaignName, String description, String segmentName, //
            List<String> modelIds, HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        tenant = tenantEntityMgr.findByTenantId(tenant.getId());
        
        List<MetadataSegment> segments = new ArrayList<>();
        for (String modelId : modelIds) {
            ModelSummary summary = modelSummaryEntityMgr.findByModelId(modelId, false, true, true);
            
            if (summary == null) {
                throw new LedpException(LedpCode.LEDP_18121, new String[] { modelId });
            }
            MetadataSegment segment = modelSegmentBuilder.createSegment(tenant.getId(), summary.getId(), summary);
            segments.add(segment);
        }

        return createCampaign(campaignName, description, tenant, segments, true);
    }

    @Override
    public Campaign createCampaignFromTable(String campaignName, String description, String segmentName, //
            String tableName, HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        Table table = metadataProxy.getTable(tenant.getId(), tableName);
        MetadataSegment segment = listSegmentBuilder.createSegment(tenant.getId(), segmentName, table);
        return createCampaign(campaignName, description, tenant, segment);
    }
    
    private Campaign createCampaign(String name, String description, Tenant tenant, MetadataSegment segment) {
        return createCampaign(name, description, tenant, Arrays.asList(new MetadataSegment[] { segment }), true);
    }
    
    private Campaign createCampaign(String name, String description, Tenant tenant, List<MetadataSegment> segments, boolean inbound) {
        Campaign campaign = new Campaign();
        campaign.setName(name);
        campaign.setDescription(description);
        campaign.setTenant(tenant);
        
        
        List<String> segmentIds = new ArrayList<>(segments.size());
        
        for (MetadataSegment s : segments) {
            segmentIds.add(s.getName());
        }
        campaign.setSegments(segmentIds);
        if (inbound) {
            campaign.setCampaignType(CampaignType.INBOUND);
        }
        campaignEntityMgr.create(campaign);
        return campaign;
    }

    @Override
    public Campaign findCampaignByName(String campaignName) {
        return campaignEntityMgr.findByName(campaignName);
    }

    @Override
    public List<Campaign> findAll() {
        return campaignEntityMgr.findAll();
    }

    @Override
    public void deleteCampaignByName(String campaignName) {
        campaignEntityMgr.deleteByName(campaignName);
    }

}
