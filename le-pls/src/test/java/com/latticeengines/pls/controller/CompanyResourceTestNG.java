package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.springframework.web.util.UriComponentsBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.Company;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class CompanyResourceTestNG extends PlsFunctionalTestNGBase {

    private static final String PLS_COMPANY_URL = "pls/companies/";
    private static final String PLS_COUNT_URL = "pls/companies/count/";
    private static final String TEST_COMPANY_ID = "113101";

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        setupUsers();
    }

    @BeforeMethod(groups = { "functional" })
    public void beforeMethod() {
        // using admin session by default
        switchToSuperAdmin();
    }

    @Test(groups = { "functional" })
    public void findById() {

        Company company = restTemplate.getForObject(getRestAPIHostPort()
                + PLS_COMPANY_URL + TEST_COMPANY_ID, Company.class);
        assertNotNull(company);
        assertEquals(company.getAccountId(), Long.valueOf(TEST_COMPANY_ID));
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = { "functional" })
    public void findByCriterias() {
        URI companiesUrl= UriComponentsBuilder.fromUriString(getRestAPIHostPort()
                + PLS_COMPANY_URL)
            .queryParam("Page", "0")
            .queryParam("Size", "16")
            .queryParam("Country", "USA")
            .queryParam("Industry", "Government#Education")
            .build()
            .toUri();
        List companies = restTemplate.getForObject(companiesUrl, List.class);

        URI countUrl= UriComponentsBuilder.fromUriString(getRestAPIHostPort()
                + PLS_COUNT_URL)
            .queryParam("Industry", "Government#Education")
            .build()
            .toUri();
        System.out.println("Final count url is " + countUrl.toString());
        Long count = restTemplate.getForObject(countUrl, Long.class);

        System.out.println("Final company count is " + count);
        assertNotNull(companies);
        assertEquals(companies.size(), 16);
        assertTrue(count > 16);
        for (int index = 0; index < 16; index++) {
            @SuppressWarnings("unchecked")
            Map<String, String> map = (Map) companies.get(index);

            String industry = map.get("Industry");
            assertTrue(industry.equals("Government") || industry.equals("Education"));
        }
    }
}
