package com.latticeengines.pls.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.Company;
import com.latticeengines.pls.entitymanager.CompanyEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;

public class CompanyEntityMgrImplTestNG extends PlsFunctionalTestNGBaseDeprecated {

    @Autowired
    private CompanyEntityMgr companyEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        // Nothing to be done right now. Don't want mess with AccountMaster.
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        // Nothing to be done right now. Don't want mess with AccountMaster.
    }

    @Test(groups = "functional")
    public void findByCompanyId() {
        Long companyId = new Long(113101);
        Company company = companyEntityMgr.findById(companyId);
        assertNotNull(company);
        assertEquals(company.getAccountId(), companyId);
    }

    @Test(groups = "functional")
    public void findByCriterias() {
        Map<String, String> criterias = new HashMap<String, String>();
        criterias.put("Page", "0");
        criterias.put("Size", "16");
        criterias.put("Country", "USA");
        criterias.put("Industry", "Government");
        List<Company> companies = companyEntityMgr.findCompanies(criterias);
        Long count = companyEntityMgr.findCompanyCount(criterias);
        assertNotNull(companies);
        assertEquals(companies.size(), 16);
        assertTrue(count.longValue() > 16);
        for (int index = 0; index < 16; index++) {
            Company company = (Company)companies.get(index);
            assertEquals(company.getCountry(), "USA");
            assertEquals(company.getIndustry(), "Government");
        }
    }

}
