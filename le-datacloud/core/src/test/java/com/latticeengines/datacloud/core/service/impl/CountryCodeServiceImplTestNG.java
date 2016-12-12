package com.latticeengines.datacloud.core.service.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.datacloud.core.service.CountryCodeService;
import com.latticeengines.datacloud.core.testframework.DataCloudCoreFunctionalTestNGBase;

@Component
public class CountryCodeServiceImplTestNG extends DataCloudCoreFunctionalTestNGBase {
    private static final Log log = LogFactory.getLog(CountryCodeServiceImplTestNG.class);
    private static final String FILENAME = "CountryCode.csv";

    @Autowired
    private CountryCodeService countryCodeService;

    @Test(groups = "functional")
    public void testSingleMapping() {
        String standardizedCountry = LocationUtils.getStandardCountry("United States");
        Assert.assertEquals(standardizedCountry, "USA");
        String countryCode = countryCodeService.getCountryCode(standardizedCountry);
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
                String standardizedCountry = LocationUtils.getStandardCountry(countryName);
                String countryCode = countryCodeService.getCountryCode(standardizedCountry);
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
        Map<String, String> standardCountries = countryCodeService.getStandardCountries();
        boolean res = true;
        String countryName;
        try {
            while ((countryName = reader.readLine()) != null) {
                String standardizedCountry = LocationUtils.getStandardCountry(countryName);
                if (!standardCountries.containsKey(standardizedCountry)) {
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
