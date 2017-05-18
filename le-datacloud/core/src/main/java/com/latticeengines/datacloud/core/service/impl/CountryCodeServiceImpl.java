package com.latticeengines.datacloud.core.service.impl;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.datacloud.core.entitymgr.CountryCodeEntityMgr;
import com.latticeengines.datacloud.core.service.CountryCodeService;

@Component("countryCodeServiceImpl")
public class CountryCodeServiceImpl implements CountryCodeService {

    private static final long serialVersionUID = 3360510922222592947L;

    private static final Log log = LogFactory.getLog(CountryCodeServiceImpl.class);

    @Autowired
    private CountryCodeEntityMgr countryCodeEntityMgr;

    private ConcurrentMap<String, String> countryCodeWhiteCache = new ConcurrentHashMap<>();
    private final ConcurrentSkipListSet<String> countryCodeBlackCache = new ConcurrentSkipListSet<>();
    private ConcurrentMap<String, String> countryWhiteCache = new ConcurrentHashMap<>();
    private final ConcurrentSkipListSet<String> countryBlackCache = new ConcurrentSkipListSet<>();

    @Autowired
    @Qualifier("commonTaskScheduler")
    private ThreadPoolTaskScheduler scheduler;

    public String getCountryCode(String country) {
        String cleanCountry = LocationUtils.getStandardCountry(country);
        if (StringUtils.isEmpty(cleanCountry)) {
            return null;
        }
        synchronized (countryCodeWhiteCache) {
            if (countryCodeWhiteCache.containsKey(cleanCountry)) {
                return countryCodeWhiteCache.get(cleanCountry);
            }
        }
        if (countryCodeBlackCache.contains(cleanCountry)) {
            return null;
        }
        String countryCode = countryCodeEntityMgr.findCountryCode(cleanCountry);
        if (countryCode == null) {
            log.info("Failed to map " + cleanCountry + " to country code");
            countryCodeBlackCache.add(cleanCountry);
        } else {
            countryCodeWhiteCache.putIfAbsent(cleanCountry, countryCode);
        }
        return countryCode;
    }

    public String getStandardCountry(String country) {
        String cleanCountry = LocationUtils.getStandardCountry(country);
        if (StringUtils.isEmpty(cleanCountry)) {
            return null;
        }
        synchronized (countryWhiteCache) {
            if (countryWhiteCache.containsKey(cleanCountry)) {
                return countryWhiteCache.get(cleanCountry);
            }
        }
        if (countryBlackCache.contains(cleanCountry)) {
            return cleanCountry;
        }
        String standardCountry = countryCodeEntityMgr.findCountry(cleanCountry);
        if (standardCountry == null) {
            countryBlackCache.add(cleanCountry);
            return cleanCountry;
        } else {
            countryWhiteCache.putIfAbsent(cleanCountry, standardCountry);
            return standardCountry;
        }
    }

    public Map<String, String> getStandardCountries() {
        return countryCodeEntityMgr.findAllCountries();
    }

    @PostConstruct
    private void postConstruct() {
        loadCache();
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                loadCache();
            }
        }, new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(30)), TimeUnit.MINUTES.toMillis(30));
    }

    private void loadCache() {
        log.info("Start loading country codes and standard countries");
        countryCodeWhiteCache = countryCodeEntityMgr.findAllCountryCodes();
        countryWhiteCache = countryCodeEntityMgr.findAllCountriesSync();
        synchronized (countryCodeBlackCache) {
            countryCodeBlackCache.clear();
        }
        synchronized (countryBlackCache) {
            countryBlackCache.clear();
        }
        log.info("Finished loading country codes and standard countries");
    }

}
