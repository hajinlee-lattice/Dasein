package com.latticeengines.pls.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.pls.entitymanager.TargetMarketEntityMgr;
import com.latticeengines.pls.service.TargetMarketService;

@Component("targetMarketService")
public class TargetMarketServiceImpl implements TargetMarketService {

    @Autowired
    private TargetMarketEntityMgr targetMarketEntityMgr;

    @Override
    public void createTargetMarket(TargetMarket targetMarket) {
        TargetMarket targetMarketStored = targetMarketEntityMgr.findTargetMarketByName(targetMarket.getName());
        if (targetMarketStored != null) {
            throw new LedpException(LedpCode.LEDP_18070, new String[] { targetMarket.getName() });
        }

        targetMarketEntityMgr.create(targetMarket);
    }

    @Override
    public void deleteTargetMarketByName(String name) {
        targetMarketEntityMgr.deleteTargetMarketByName(name);
    }

    @Override
    public TargetMarket getTargetMarketByName(String name) {
        return targetMarketEntityMgr.findTargetMarketByName(name);
    }

    @Override
    public List<TargetMarket> getAllTargetMarkets() {
        return targetMarketEntityMgr.getAllTargetMarkets();
    }

    @Override
    public void updateTargetMarketByName(TargetMarket targetMarket, String name) {
        targetMarketEntityMgr.updateTargetMarketByName(targetMarket, name);
    }

    @Override
    public TargetMarket createDefaultTargetMarket() {
        List<TargetMarket> targetMarkets = targetMarketEntityMgr.findAll();
        
        for (TargetMarket targetMarket : targetMarkets) {
            if (targetMarket.getIsDefault()) {
               throw new LedpException(LedpCode.LEDP_18070);
            }
        }
        return targetMarketEntityMgr.createDefaultTargetMarket();
    }
    
}
