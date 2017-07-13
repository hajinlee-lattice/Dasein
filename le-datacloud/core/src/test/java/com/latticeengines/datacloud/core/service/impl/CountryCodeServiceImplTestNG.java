package com.latticeengines.datacloud.core.service.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.service.CountryCodeService;
import com.latticeengines.datacloud.core.testframework.DataCloudCoreFunctionalTestNGBase;

@Component
public class CountryCodeServiceImplTestNG extends DataCloudCoreFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(CountryCodeServiceImplTestNG.class);
    private static final String FILENAME = "CountryCode.csv";

    @Autowired
    private CountryCodeService countryCodeService;

    @Test(groups = "functional")
    public void testSingleMapping() {
        String countryCode = countryCodeService.getCountryCode("United States");
        Assert.assertEquals(countryCode, "US");
    }

    @Test(groups = "functional")
    public void bulkTest() {
        InputStream fileStream = ClassLoader.getSystemResourceAsStream("datasource/" + FILENAME);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileStream));
        boolean res = true;
        String countryName;
        try {
            while ((countryName = reader.readLine()) != null) {
                String countryCode = countryCodeService.getCountryCode(countryName);
                if (countryCode == null) {
                    log.info("Input: " + countryName);
                    res = false;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        Assert.assertTrue(res);
    }

    @Test(groups = "functional")
    public void testCountryStandardization() {
        InputStream fileStream = ClassLoader.getSystemResourceAsStream("datasource/" + FILENAME);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileStream));
        Map<String, String> countryMap = countryCodeService.getStandardCountries();
        Set<String> standardCountries = new HashSet<>();
        for (String standardCountry : countryMap.values()) {
            standardCountries.add(standardCountry);
        }
        boolean res = true;
        String countryName;
        try {
            while ((countryName = reader.readLine()) != null) {
                String standardizedCountry = countryCodeService.getStandardCountry(countryName);
                if (!standardCountries.contains(standardizedCountry)) {
                    log.info(String.format("Fail to standardize country: standardizedCountry = %s, countryName = %s",
                            standardizedCountry, countryName));
                    res = false;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        Assert.assertTrue(res);
    }
}
