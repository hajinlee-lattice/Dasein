package com.latticeengines.datacloud.match.service.impl;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.entitymgr.CountryCodeEntityMgr;
import com.latticeengines.datacloud.match.service.CountryCodeService;

@Component("countryCodeServiceImpl")
public class CountryCodeServiceImpl implements CountryCodeService {

    private static final long serialVersionUID = 3360510922222592947L;

    private static final Log log = LogFactory.getLog(CountryCodeServiceImpl.class);

    @Autowired
    private CountryCodeEntityMgr countryCodeEntityMgr;

    private ConcurrentMap<String, String> countryCodeMap;

    @Autowired
    @Qualifier("taskScheduler")
    private ThreadPoolTaskScheduler scheduler;

    public String getCountryCode(String standardizedCountry) {
        if (!countryCodeMap.containsKey(standardizedCountry)) {
            log.info("Failed to map " + standardizedCountry + " to country code");
            return null;
        } else {
            return countryCodeMap.get(standardizedCountry);
        }
    }

    @PostConstruct
    private void postConstruct() {
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                loadCache();
            }
        }, TimeUnit.MINUTES.toMillis(10));
    }

    private void loadCache() {
        log.info("Start loading country code");
        countryCodeMap = countryCodeEntityMgr.findAll();
        log.info("Loading country code finished");
    }

}
