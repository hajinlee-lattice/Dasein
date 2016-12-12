package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMasterSeed;
import com.latticeengines.datacloud.core.source.impl.DnBCacheSeed;
import com.latticeengines.datacloud.core.source.impl.LatticeCacheSeed;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;

public class AccountMasterSeedRebuildServiceImplTestNG
        extends TransformationServiceImplTestNGBase<BasicTransformationConfiguration> {
    private static final Log log = LogFactory.getLog(AccountMasterSeedRebuildServiceImplTestNG.class);

    private String baseSourceVersionDnB = "2016-10-01_00-00-00_UTC";
    private String baseSourceVersionLattice = "2016-09-01_00-00-00_UTC";
    private String targetVersion = "2016-10-01_00-00-00_UTC";

    private static final String LATTICEID = "LatticeID";
    private static final String DUNS = "DUNS";
    private static final String DOMAIN = "Domain";
    private static final String NAME = "Name";
    private static final String STREET = "Street";
    private static final String CITY = "City";
    private static final String STATE = "State";
    private static final String COUNTRY = "Country";
    private static final String ZIPCODE = "ZipCode";
    private static final String LE_IS_PRIMARY_DOMAIN = "LE_IS_PRIMARY_DOMAIN";
    private static final String LE_IS_PRIMARY_LOCATION = "LE_IS_PRIMARY_LOCATION";
    private static final String LE_NUMBER_OF_LOCATIONS = "LE_NUMBER_OF_LOCATIONS";
    private static final String LE_PRIMARY_DUNS = "LE_PRIMARY_DUNS";
    private static final String LE_COMPANY_DESCRIPTION = "LE_COMPANY_DESCRIPTION";
    private static final String LE_COMPANY_PHONE = "LE_COMPANY_PHONE";
    private static final String LE_SIC_CODE = "LE_SIC_CODE";
    private static final String LE_NAICS_CODE = "LE_NAICS_CODE";
    private static final String LE_INDUSTRY = "LE_INDUSTRY";
    private static final String LE_REVENUE_RANGE = "LE_REVENUE_RANGE";
    private static final String LE_EMPLOYEE_RANGE = "LE_EMPLOYEE_RANGE";
    private static final String LE_COUNTRY = "LE_COUNTRY";

    @Autowired
    AccountMasterSeed source;

    @Autowired
    DnBCacheSeed dnBCacheSeed;

    @Autowired
    LatticeCacheSeed latticeCacheSeed;

    @Autowired
    private AccountMasterSeedRebuildService accountMasterSeedRebuildService;

    @Test(groups = "functional")
    public void testTransformation() {
        uploadBaseAvro(dnBCacheSeed, dnBCacheSeed.getSourceName() + "_Test" + source.getSourceName(),
                baseSourceVersionDnB);
        uploadBaseAvro(latticeCacheSeed, baseSourceVersionLattice);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    TransformationService<BasicTransformationConfiguration> getTransformationService() {
        return accountMasterSeedRebuildService;
    }

    @Override
    Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return null;
    }

    @Override
    BasicTransformationConfiguration createTransformationConfiguration() {
        BasicTransformationConfiguration configuration = new BasicTransformationConfiguration();
        configuration.setVersion(targetVersion);
        List<String> baseVersions = new ArrayList<String>();
        baseVersions.add(baseSourceVersionDnB);
        baseVersions.add(baseSourceVersionLattice);
        configuration.setBaseVersions(baseVersions);
        return configuration;
    }

    @Override
    protected String getPathForResult() {
        return hdfsPathBuilder.constructSnapshotDir(source, targetVersion).toString();
    }

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Long latticeId = (Long) record.get(LATTICEID);
            String duns = String.valueOf(record.get(DUNS));
            String domain = String.valueOf(record.get(DOMAIN));
            String name = String.valueOf(record.get(NAME));
            String street = String.valueOf(record.get(STREET));
            String city = String.valueOf(record.get(CITY));
            String state = String.valueOf(record.get(STATE));
            String country = String.valueOf(record.get(COUNTRY));
            String zipcode = String.valueOf(record.get(ZIPCODE));
            String isPrimaryDomain = String.valueOf(record.get(LE_IS_PRIMARY_DOMAIN));
            String isPrimaryLocation = String.valueOf(record.get(LE_IS_PRIMARY_LOCATION));
            Integer numberOfLocations = (Integer) record.get(LE_NUMBER_OF_LOCATIONS);
            String primaryDuns = String.valueOf(record.get(LE_PRIMARY_DUNS));
            String companyDescription = String.valueOf(record.get(LE_COMPANY_DESCRIPTION));
            String companyPhone = String.valueOf(record.get(LE_COMPANY_PHONE));
            String sicCode = String.valueOf(record.get(LE_SIC_CODE));
            String naicsCode = String.valueOf(record.get(LE_NAICS_CODE));
            String industry = String.valueOf(record.get(LE_INDUSTRY));
            String revenueRange = String.valueOf(record.get(LE_REVENUE_RANGE));
            String employeeRange = String.valueOf(record.get(LE_EMPLOYEE_RANGE));

            log.info(latticeId + " " + duns + " " + domain + " " + name + " " + street + " " + city + " " + state + " "
                    + country + " " + zipcode + " " + isPrimaryDomain + " " + isPrimaryLocation + " "
                    + numberOfLocations + " " + primaryDuns + " " + companyDescription + " " + companyPhone + " "
                    + sicCode + " " + naicsCode + " " + industry + " " + revenueRange + " " + employeeRange);

            Assert.assertTrue(
                            (primaryDuns.equals("DnB_04_PRIMARY_DUNS")
                            && companyPhone.equals("DnB_04_COMPANY_PHONE")
                                    && employeeRange.equals("DnB_04_EMPLOYEE_RANGE")
                                    && companyDescription.equals("DnB_04_COMPANY_DESCRIPTION")
                                    && zipcode.equals("DnB_04_ZIPCODE") && numberOfLocations == 6
                                    && sicCode.equals("DnB_04_SIC_CODE") && isPrimaryLocation.equals("Y")
                                    && city.equals("DnB_04_CITY") && industry.equals("DnB_04_INDUSTRY")
                                    && name.equals("DnB_04_NAME") && duns.equals("04") && state.equals("DnB_04_STATE")
                                    && naicsCode.equals("DnB_04_NAICS_CODE")
                                    && revenueRange.equals("DnB_04_REVENUE_RANGE")
                                    && street.equals("DnB_04_ADDR")
                            && country.equals("GERMANY") && domain.equals("null")
                                    && isPrimaryDomain.equals("N"))
                            || (primaryDuns.equals("DnB_01_PRIMARY_DUNS")
                                    && companyPhone.equals("DnB_01_COMPANY_PHONE")
                                    && employeeRange.equals("DnB_01_EMPLOYEE_RANGE")
                                    && companyDescription.equals("DnB_01_COMPANY_DESCRIPTION")
                                    && zipcode.equals("DnB_01_ZIPCODE") && numberOfLocations == 3
                                    && sicCode.equals("DnB_01_SIC_CODE") && isPrimaryLocation.equals("Y")
                                    && city.equals("DnB_01_CITY") && industry.equals("DnB_01_INDUSTRY")
                                    && name.equals("DnB_01_NAME") && duns.equals("01") && state.equals("DnB_01_STATE")
                                    && naicsCode.equals("DnB_01_NAICS_CODE")
                                    && revenueRange.equals("DnB_01_REVENUE_RANGE")
                                    && street.equals("DnB_01_ADDR") && country.equals("USA")
                                    && domain.equals("e.com") && isPrimaryDomain.equals("N"))
                            || (primaryDuns.equals("DnB_01_PRIMARY_DUNS")
                                    && companyPhone.equals("DnB_01_COMPANY_PHONE")
                                    && employeeRange.equals("DnB_01_EMPLOYEE_RANGE")
                                    && companyDescription.equals("DnB_01_COMPANY_DESCRIPTION")
                                    && zipcode.equals("DnB_01_ZIPCODE") && numberOfLocations == 3
                                    && sicCode.equals("DnB_01_SIC_CODE") && isPrimaryLocation.equals("Y")
                                    && city.equals("DnB_01_CITY") && industry.equals("DnB_01_INDUSTRY")
                                    && name.equals("DnB_01_NAME") && duns.equals("01") && state.equals("DnB_01_STATE")
                                    && naicsCode.equals("DnB_01_NAICS_CODE")
                                    && revenueRange.equals("DnB_01_REVENUE_RANGE")
                                    && street.equals("DnB_01_ADDR") && country.equals("USA") 
                                    && domain.equals("b.com") && isPrimaryDomain.equals("N"))
                            || (primaryDuns.equals("DnB_03_PRIMARY_DUNS")
                                    && companyPhone.equals("DnB_03_COMPANY_PHONE")
                                    && employeeRange.equals("DnB_03_EMPLOYEE_RANGE")
                                    && companyDescription.equals("DnB_03_COMPANY_DESCRIPTION")
                                    && zipcode.equals("DnB_03_ZIPCODE") && numberOfLocations == 5
                                    && sicCode.equals("DnB_03_SIC_CODE") && isPrimaryLocation.equals("Y")
                                    && city.equals("DnB_03_CITY") && industry.equals("DnB_03_INDUSTRY")
                                    && name.equals("DnB_03_NAME") && duns.equals("03") && state.equals("DnB_03_STATE")
                                    && naicsCode.equals("DnB_03_NAICS_CODE")
                                    && revenueRange.equals("DnB_03_REVENUE_RANGE") && street.equals("DnB_03_ADDR")
                                    && country.equals("CHINA") && domain.equals("a.com")
                                    && isPrimaryDomain.equals("Y"))
                            || (primaryDuns.equals("DnB_01_PRIMARY_DUNS")
                                    && companyPhone.equals("DnB_01_COMPANY_PHONE")
                                    && employeeRange.equals("DnB_01_EMPLOYEE_RANGE")
                                    && companyDescription.equals("DnB_01_COMPANY_DESCRIPTION")
                                    && zipcode.equals("DnB_01_ZIPCODE") && numberOfLocations == 3
                                    && sicCode.equals("DnB_01_SIC_CODE") && isPrimaryLocation.equals("Y")
                                    && city.equals("DnB_01_CITY") && industry.equals("DnB_01_INDUSTRY")
                                    && name.equals("DnB_01_NAME") && duns.equals("01") && state.equals("DnB_01_STATE")
                                    && naicsCode.equals("DnB_01_NAICS_CODE")
                                    && revenueRange.equals("DnB_01_REVENUE_RANGE")
                                    && street.equals("DnB_01_ADDR") && country.equals("USA")
                                    && domain.equals("c.com") && isPrimaryDomain.equals("Y"))
                            || (primaryDuns.equals("DnB_01_PRIMARY_DUNS")
                                    && companyPhone.equals("DnB_01_COMPANY_PHONE")
                                    && employeeRange.equals("DnB_01_EMPLOYEE_RANGE")
                                    && companyDescription.equals("DnB_01_COMPANY_DESCRIPTION")
                                    && zipcode.equals("DnB_01_ZIPCODE") && numberOfLocations == 3
                                    && sicCode.equals("DnB_01_SIC_CODE") && isPrimaryLocation.equals("Y")
                                    && city.equals("DnB_01_CITY") && industry.equals("DnB_01_INDUSTRY")
                                    && name.equals("DnB_01_NAME") && duns.equals("01") && state.equals("DnB_01_STATE")
                                    && naicsCode.equals("DnB_01_NAICS_CODE")
                                    && revenueRange.equals("DnB_01_REVENUE_RANGE") && street.equals("DnB_01_ADDR")
                                    && country.equals("USA") && domain.equals("a.com")
                                    && isPrimaryDomain.equals("N"))
                            || (primaryDuns.equals("null")
                                    && companyPhone.equals("null") && employeeRange.equals("null")
                                    && companyDescription.equals("null") && zipcode.equals("null")
                                    && numberOfLocations == 1 && sicCode.equals("null") && isPrimaryLocation.equals("Y")
                                    && city.equals("LE_NULL_CITY_2") && industry.equals("null")
                                    && name.equals("LE_NULL_NAME_2") && duns.equals("null")
                                    && state.equals("LE_NULL_STATE_2") && naicsCode.equals("null")
                                    && revenueRange.equals("null") && street.equals("null")
                                    && country.equals("BRAZIL") && domain.equals("d.com") && isPrimaryDomain.equals("Y"))
                            || (primaryDuns.equals("DnB_02_PRIMARY_DUNS")
                            && companyPhone.equals("DnB_02_COMPANY_PHONE")
                                    && employeeRange.equals("DnB_02_EMPLOYEE_RANGE")
                                    && companyDescription.equals("DnB_02_COMPANY_DESCRIPTION")
                                    && zipcode.equals("DnB_02_ZIPCODE") && numberOfLocations == 4
                                    && sicCode.equals("DnB_02_SIC_CODE") && isPrimaryLocation.equals("Y")
                                    && city.equals("DnB_02_CITY") && industry.equals("DnB_02_INDUSTRY")
                                    && name.equals("DnB_02_NAME") && duns.equals("02") && state.equals("DnB_02_STATE")
                                    && naicsCode.equals("DnB_02_NAICS_CODE") && revenueRange.equals("DnB_02_REVENUE_RANGE")
                                    && street.equals("DnB_02_ADDR") && country.equals("CANADA")
                                    && domain.equals("a.com") && isPrimaryDomain.equals("Y")));
            rowNum = rowNum + 1;
        }
        Assert.assertEquals(rowNum, 8);
    }
}
