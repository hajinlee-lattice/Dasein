package com.latticeengines.pls.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.ulysses.Campaign;

public interface CampaignEntityMgr extends BaseEntityMgr<Campaign> {

    void deleteByName(String campaignName);
    
    Campaign findByName(String campaignName);
}
