package com.latticeengines.network.exposed.pls;

import java.util.List;

import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.pls.TargetMarket;

public interface TargetMarketInterface {

    void create(TargetMarket targetMarket);

    TargetMarket createDefault();

    void delete(String targetMarketName);

    void update(String targetMarketName, TargetMarket targetMarket);

    TargetMarket find(String targetMarketName);

    void registerReport(String targetMarketName, Report report);

    List<TargetMarket> findAll();
}
