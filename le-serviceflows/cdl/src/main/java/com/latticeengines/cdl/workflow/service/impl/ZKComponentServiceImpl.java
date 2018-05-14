package com.latticeengines.cdl.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.cdl.service.ApsRollingPeriod;
import com.latticeengines.cdl.service.ZKComponentService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;

@Component("zkComponentService")
public class ZKComponentServiceImpl implements ZKComponentService {

    private static final String APS_ROLLUP_PERIOD = "DefaultAPSRollupPeriod";
    private static final String CDL_SERVICE = "CDL";
    private static final Logger log = LoggerFactory.getLogger(ZKComponentServiceImpl.class);
    private static Map<ApsRollingPeriod, PeriodStrategy> periodStragegyMap;
    static {
        periodStragegyMap = new HashMap<>();
        periodStragegyMap.put(ApsRollingPeriod.BUSINESS_WEEK, PeriodStrategy.CalendarWeek);
        periodStragegyMap.put(ApsRollingPeriod.BUSINESS_MONTH, PeriodStrategy.CalendarMonth);
        periodStragegyMap.put(ApsRollingPeriod.BUSINESS_QUARTER, PeriodStrategy.CalendarQuarter);
        periodStragegyMap.put(ApsRollingPeriod.BUSINESS_YEAR, PeriodStrategy.CalendarYear);
    }

    @Override
    public PeriodStrategy getRollingPeriod(CustomerSpace customerSpace) {
        try {
            Path cdlPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), customerSpace,
                    CDL_SERVICE);
            Path periodPath = cdlPath.append(APS_ROLLUP_PERIOD);
            Camille camille = CamilleEnvironment.getCamille();
            if (camille.exists(periodPath)) {
                String periodName = camille.get(periodPath).getData();
                ApsRollingPeriod period = ApsRollingPeriod.fromName(periodName);
                PeriodStrategy periodStrategy = periodStragegyMap.get(period);
                return periodStrategy != null ? periodStrategy : PeriodStrategy.CalendarMonth;
            }
        } catch (Exception e) {
            log.warn("Failed to get rolling period from ZK for " + customerSpace.getTenantId() + ", error="
                    + e.getMessage());
        }
        return PeriodStrategy.CalendarMonth;
    }

}
