package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.Report;
import com.latticeengines.domain.exposed.pls.TargetMarket;

public interface TargetMarketService {

    void createTargetMarket(TargetMarket targetMarket);

    void deleteTargetMarketByName(String name);

    TargetMarket findTargetMarketByName(String name);

    List<TargetMarket> findAllTargetMarkets();

    void updateTargetMarketByName(TargetMarket targetMarket, String name);

    TargetMarket createDefaultTargetMarket();

    void registerReport(String targetMarketName, Report report);
    
    Boolean resetDefaultTargetMarket();

}
