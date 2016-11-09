package com.latticeengines.pls.dao.impl;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.ulysses.Campaign;
import com.latticeengines.pls.dao.CampaignDao;

@Component("campaignDao")
public class CampaignDaoImpl extends BaseDaoImpl<Campaign> implements CampaignDao {

    @Override
    protected Class<Campaign> getEntityClass() {
        return Campaign.class;
    }

    @Override
    public void deleteAll() {
        // The use of CollectionTable for segments in Campaign object requires that no cascade delete is created
        // at the database level.
        List<Campaign> campaigns = findAll();
        for (Campaign campaign : campaigns) {
            delete(campaign);
        }
    }


}
