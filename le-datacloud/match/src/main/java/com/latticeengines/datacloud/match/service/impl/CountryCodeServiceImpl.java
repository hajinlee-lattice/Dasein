package com.latticeengines.datacloud.match.service.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
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

    private ConcurrentMap<String, String> countryCodeWhiteCache = new ConcurrentHashMap<>();
    private final ConcurrentSkipListSet<String> countryCodeBlackCache = new ConcurrentSkipListSet<>();

    @Autowired
    @Qualifier("taskScheduler")
    private ThreadPoolTaskScheduler scheduler;

    public String getCountryCode(String standardizedCountry) {
        if (countryCodeWhiteCache.containsKey(standardizedCountry)) {
            return countryCodeWhiteCache.get(standardizedCountry);
        }
        if (countryCodeBlackCache.contains(standardizedCountry)) {
            return null;
        }
        String countryCode = countryCodeEntityMgr.findByCountry(standardizedCountry);
        if (countryCode == null) {
            log.info("Failed to map " + standardizedCountry + " to country code");
            synchronized (countryCodeBlackCache) {
                countryCodeBlackCache.add(standardizedCountry);
            }
        } else {
            synchronized (countryCodeWhiteCache) {
                countryCodeWhiteCache.putIfAbsent(standardizedCountry, countryCode);
            }
        }
        return countryCode;
    }

    @PostConstruct
    private void postConstruct() {
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                loadCache();
            }
        }, TimeUnit.MINUTES.toMillis(30));
    }

    private void loadCache() {
        log.info("Start loading country code");
        countryCodeWhiteCache = countryCodeEntityMgr.findAll();
        synchronized (countryCodeBlackCache) {
            countryCodeBlackCache.clear();
        }
        log.info("Finished loading country code");
    }

}
