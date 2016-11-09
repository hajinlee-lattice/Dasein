package com.latticeengines.pls.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.HibernateUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.ulysses.Campaign;
import com.latticeengines.pls.dao.CampaignDao;
import com.latticeengines.pls.entitymanager.CampaignEntityMgr;

@Component("campaignEntityMgr")
public class CampaignEntityMgrImpl extends BaseEntityMgrImpl<Campaign> implements CampaignEntityMgr {

    @Autowired
    private CampaignDao campaignDao;
    
    @Override
    public BaseDao<Campaign> getDao() {
        return campaignDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteByName(String campaignName) {
        Campaign campaign = findByName(campaignName);
        campaignDao.delete(campaign);
    }
    
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Campaign findByName(String campaignName) {
        Campaign campaign = campaignDao.findByField("NAME", campaignName);
        HibernateUtils.inflateDetails(campaign.getSegments());
        HibernateUtils.inflateDetails(campaign.getInsights());
        return campaign;
    }

}
