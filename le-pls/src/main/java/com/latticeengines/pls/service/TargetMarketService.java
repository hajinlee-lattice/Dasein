package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.TargetMarket;

public interface TargetMarketService {

    void createTargetMarket(TargetMarket targetMarket);

    void deleteTargetMarketByName(String name);

    TargetMarket getTargetMarketByName(String name);

    List<TargetMarket> getAllTargetMarkets();

    void updateTargetMarketByName(TargetMarket targetMarket, String name);

}
