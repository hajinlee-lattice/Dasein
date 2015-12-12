package com.latticeengines.domain.exposed.pls;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class CompanyUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() {
        Company company = new Company();
        
        company.setPid(new Long(123));
        company.setName("xyz");
        company.setDomain("abc");
        company.setStreet("bcd");
        company.setCity("cde");
        company.setState("def");
        company.setRegion("efg");
        company.setCountry("fgh");
        company.setIndustry("ghi");
        company.setSubIndustry("hij");
        company.setRevenueRange("ijk");
        company.setEmployeesRange("jkl");
        company.setParentId(new Long(234));
        company.setInsideViewId(new Integer(456));
        
        String serializedStr = company.toString();
        System.out.println(serializedStr);
        Company deserializedCompany = JsonUtils.deserialize(serializedStr, Company.class);
        
        assertEquals(deserializedCompany.getAccountId(), company.getAccountId());
        assertEquals(deserializedCompany.getName(), company.getName());
        assertEquals(deserializedCompany.getDomain(), company.getDomain());
        assertEquals(deserializedCompany.getParentId(), company.getParentId());
        assertEquals(deserializedCompany.getCountry(), company.getCountry());
        assertEquals(deserializedCompany.getInsideViewId(), company.getInsideViewId());
    }
}
