package com.latticeengines.pls.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.pls.entitymanager.TargetMarketEntityMgr;
import com.latticeengines.pls.service.TargetMarketService;

@Component("targetMarketService")
public class TargetMarketServiceImpl implements TargetMarketService {

    @Autowired
    TargetMarketEntityMgr targetMarketEntityMgr;

    @Override
    public void createTargetMarket(TargetMarket targetMarket) {
        this.targetMarketEntityMgr.create(targetMarket);
    }

    @Override
    public void deleteTargetMarketByName(String name) {
        this.targetMarketEntityMgr.deleteTargetMarketByName(name);
    }

    @Override
    public TargetMarket getTargetMarketByName(String name) {
        return this.targetMarketEntityMgr.findTargetMarketByName(name);
    }

    @Override
    public List<TargetMarket> getAllTargetMarkets() {
        return this.targetMarketEntityMgr.getAllTargetMarkets();
    }

    @Override
    public void updateTargetMarketByName(TargetMarket targetMarket, String name) {
        this.targetMarketEntityMgr.updateTargetMarketByName(targetMarket, name);
    }

}
