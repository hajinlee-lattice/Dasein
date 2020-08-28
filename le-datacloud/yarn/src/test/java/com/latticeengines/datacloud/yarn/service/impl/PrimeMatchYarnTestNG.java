package com.latticeengines.datacloud.yarn.service.impl;

import static com.latticeengines.domain.exposed.datacloud.match.config.ExclusionCriterion.OutOfBusiness;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.yarn.exposed.service.DataCloudYarnService;
import com.latticeengines.datacloud.yarn.testframework.DataCloudYarnFunctionalTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusMatchConfig;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusMatchRule;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

public class PrimeMatchYarnTestNG extends DataCloudYarnFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PrimeMatchYarnTestNG.class);

    private static final String avroDir = "/tmp/PrimeMatchYarnTestNG";
    private static final String podId = "PrimeMatchYarnTestNG";

    @Inject
    private DataCloudYarnService dataCloudYarnService;

    @Inject
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Value("${datacloud.match.default.decision.graph.prime}")
    private String primeMatchDG;

    @BeforeClass(groups = {"functional", "manual"})
    public void setup() throws Exception {
        switchHdfsPod(podId);
        if (HdfsUtils.fileExists(yarnConfiguration, hdfsPathBuilder.podDir().toString())) {
            HdfsUtils.rmdir(yarnConfiguration, hdfsPathBuilder.podDir().toString());
        }
    }

    @Test(groups = "functional")
    public void testPrimeMatch() {
        String fileName = "BulkMatchInput.avro";
        cleanupAvroDir(avroDir);
        uploadDataCsv(avroDir, fileName);
        String avroPath = avroDir + "/" + fileName;

        DataCloudJobConfiguration jobConfiguration = jobConfiguration(avroPath);

        ApplicationId applicationId = dataCloudYarnService.submitPropDataJob(jobConfiguration);
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnClient, applicationId);
        Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        String avroGlob = getBlockOutputDir(jobConfiguration) + "/*.avro";
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlob);
        log.info("Fields: {}", StringUtils.join(schema.getFields().stream() //
                .map(Schema.Field::name).collect(Collectors.toList()), ","));
        Iterator<GenericRecord> records = AvroUtils.iterateAvroFiles(yarnConfiguration, avroGlob);
        long count = 0L;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Assert.assertNotNull(record.get(InterfaceName.InternalId.name()));
            // System.out.println(record);
            if (record.get("controlownershipdate") != null) {
                String shipDate = record.get("controlownershipdate").toString();
                Assert.assertTrue(Integer.parseInt(shipDate) > 1900);
            }
            count++;
        }
        Assert.assertTrue(count > 0);
    }

    private String getBlockOutputDir(DataCloudJobConfiguration jobConfiguration) {
        String rootUid = jobConfiguration.getRootOperationUid();
        String blockUid = jobConfiguration.getBlockOperationUid();
        return hdfsPathBuilder.constructMatchBlockDir(rootUid, blockUid).toString();
    }

    private DataCloudJobConfiguration jobConfiguration(String avroPath) {
        Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(avroPath));
        Map<MatchKey, List<String>> keyMap = MatchKeyUtils.resolveKeyMap(schema);

        MatchInput matchInput = new MatchInput();

        matchInput.setTenant(new Tenant(DataCloudConstants.SERVICE_TENANT));
        matchInput.setCustomSelection(getColumnSelection());
        matchInput.setDataCloudVersion(versionEntityMgr.currentApprovedVersionAsString());
        matchInput.setKeyMap(keyMap);
        matchInput.setSplitsPerBlock(8);
        matchInput.setDecisionGraph(primeMatchDG);
        matchInput.setUseDnBCache(false);
        matchInput.setUseRemoteDnB(true);
        matchInput.setAllocateId(false);
        matchInput.setEntityKeyMaps(prepareEntityKeyMap());
        matchInput.setTargetEntity(BusinessEntity.PrimeAccount.name());
        matchInput.setOperationalMode(OperationalMode.LDC_MATCH);
        DplusMatchRule baseRule = new DplusMatchRule(7, Collections.singleton("A.{3}A.{3}.*"))
                .exclude(OutOfBusiness) //
                .review(4, 6, Collections.singleton("A.*"));
        DplusMatchConfig dplusMatchConfig = new DplusMatchConfig(baseRule);
        matchInput.setDplusMatchConfig(dplusMatchConfig);
        matchInput.setUseDirectPlus(true);

        DataCloudJobConfiguration jobConfiguration = new DataCloudJobConfiguration();
        jobConfiguration.setHdfsPodId(podId);
        jobConfiguration.setName("DataCloudMatchBlock");
        jobConfiguration.setCustomerSpace(CustomerSpace.parse("LDCTest"));
        jobConfiguration.setAvroPath(avroPath);
        jobConfiguration.setBlockSize(AvroUtils.count(yarnConfiguration, avroPath).intValue());
        jobConfiguration.setRootOperationUid(UUID.randomUUID().toString().toUpperCase());
        jobConfiguration.setBlockOperationUid(UUID.randomUUID().toString().toUpperCase());
        jobConfiguration.setThreadPoolSize(4);
        jobConfiguration.setGroupSize(10);
        jobConfiguration.setMatchInput(matchInput);

        return jobConfiguration;
    }

    private Map<String, MatchInput.EntityKeyMap> prepareEntityKeyMap() {
        Map<String, MatchInput.EntityKeyMap> entityKeyMaps = new HashMap<>();
        // Both Account & Contact match needs Account key map
        MatchInput.EntityKeyMap entityKeyMap = new MatchInput.EntityKeyMap();
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.DUNS, Collections.singletonList("DUNS"));
        keyMap.put(MatchKey.Name, Collections.singletonList("CompanyName"));
        keyMap.put(MatchKey.State, Collections.singletonList("State"));
        keyMap.put(MatchKey.Country, Collections.singletonList("Country"));
        entityKeyMap.setKeyMap(keyMap);
        entityKeyMaps.put(BusinessEntity.Account.name(), entityKeyMap);
        return entityKeyMaps;
    }

    private ColumnSelection getColumnSelection() {
        List<Column> columns = Stream.of( //
                "duns_number", //
                "primaryname", //
                "countryisoalpha2code", //
                "activities_desc", //
                "activities_language_desc", //
                "activities_language_code", //
                "dunscontrolstatus_isdelisted", //
                "dunscontrolstatus_ismailundeliverable", //
                "dunscontrolstatus_ismarketable", //
                "dunscontrolstatus_istelephonedisconnected", //
                "dunscontrolstatus_operatingstatus_desc", //
                "dunscontrolstatus_operatingstatus_code", //
                "dunscontrolstatus_subjecthandling_desc", //
                "dunscontrolstatus_subjecthandling_code", //
                "email_addr", //
                "isnonclassifiedestablishment", //
                "primaryaddr_country_isoalpha2code", //
                "primaryaddr_country_name", //
                "primaryaddr_county_name", //
                "primaryaddr_addrlocality_name", //
                "primaryaddr_addrregion_abbreviatedname", //
                "primaryaddr_addrregion_name", //
                "primaryaddr_continentalregion_name", //
                "primaryaddr_isregisteredaddr", //
                "primaryaddr_language_desc", //
                "primaryaddr_language_code", //
                "primaryaddr_minortownname", //
                "primaryaddr_postalcode", //
                "primaryaddr_postalcodeposition_desc", //
                "primaryaddr_postalcodeposition_code", //
                "primaryaddr_postofficebox_postofficeboxnumber", //
                "primaryaddr_postofficebox_typedesc", //
                "primaryaddr_postofficebox_typecode", //
                "primaryaddr_street_line1", //
                "primaryaddr_street_line2", //
                "primaryaddr_streetname", //
                "primaryaddr_streetnumber", //
                "primaryindcode_ussicv4", //
                "primaryindcode_ussicv4desc", //
                "registrationnumbers_ispreferredregistrationnumber", //
                "registrationnumbers_registrationnumber", //
                "registrationnumbers_typedesc", //
                "registrationnumbers_typecode", //
                "stockexchanges_exchangename_desc", //
                "stockexchanges_isprimary", //
                "stockexchanges_tickername", //
                "telephone_isdcode", //
                "telephone_telephonenumber", //
                "tradestylenames_name", //
                "tradestylenames_priority", //
                "unspsccodes_code", //
                "unspsccodes_desc", //
                "unspsccodes_priority", //
                "website_domainname", //
                "website_url", //
                "banks_addr_addrlocality_name", //
                "banks_addr_addrregion_name", //
                "banks_addr_postalcode", //
                "banks_addr_street_line1", //
                "banks_addr_street_line2", //
                "banks_name", //
                "banks_registrationnumbers_registrationnumber", //
                "banks_telephone_telephonenumber", //
                "businessentitytype_desc", //
                "businessentitytype_code", //
                "chartertype_desc", //
                "chartertype_code", //
                "controlownershipdate", //
                "controlownershiptype_desc", //
                "controlownershiptype_code", //
                "dunscontrolstatus_operatingstatus_startdate", //
                "dunscontrolstatus_recordclass_desc", //
                "dunscontrolstatus_recordclass_code", //
                "fin_fintatementduration", //
                "fin_fintatementtodate", //
                "fin_infoscopedesc", //
                "fin_infoscopecode", //
                "fin_reliabilitydesc", //
                "fin_reliabilitycode", //
                "fin_unitcode", //
                "fin_yrrevenue_currency", //
                "fin_yrrevenue_val", //
                "franchiseoperationtype_desc", //
                "franchiseoperationtype_code", //
                "incorporateddate", //
                "indcodes_code", //
                "indcodes_desc", //
                "indcodes_priority", //
                "indcodes_typedesc", //
                "indcodes_typecode", //
                "isagent", //
                "isexporter", //
                "isforbeslargestprivatecompanieslisted", //
                "isfortune1000listed", //
                "isimporter", //
                "isstandalone", //
                "legalform_desc", //
                "legalform_code", //
                "legalform_registrationlocation_addrregion", //
                "legalform_startdate", //
                "mailingaddr_country_isoalpha2code", //
                "mailingaddr_country_name", //
                "mailingaddr_county_name", //
                "mailingaddr_addrlocality_name", //
                "mailingaddr_addrregion_abbreviatedname", //
                "mailingaddr_addrregion_name", //
                "mailingaddr_continentalregion_name", //
                "mailingaddr_language_desc", //
                "mailingaddr_language_code", //
                "mailingaddr_minortownname", //
                "mailingaddr_postalcode", //
                "mailingaddr_postalcodeposition_desc", //
                "mailingaddr_postalcodeposition_code", //
                "mailingaddr_postalroute", //
                "mailingaddr_postofficebox_postofficeboxnumber", //
                "mailingaddr_postofficebox_typedesc", //
                "mailingaddr_postofficebox_typecode", //
                "mailingaddr_street_line1", //
                "mailingaddr_street_line2", //
                "mailingaddr_streetname", //
                "mailingaddr_streetnumber", //
                "numberofemployees_employeecategories_employmentbasisdesc", //
                "numberofemployees_employeecategories_employmentbasiscode", //
                "numberofemployees_employeefiguresdate", //
                "numberofemployees_infoscopedesc", //
                "numberofemployees_infoscopecode", //
                "numberofemployees_reliabilitydesc", //
                "numberofemployees_reliabilitycode", //
                "numberofemployees_trend_gr", //
                "numberofemployees_trend_reliabilitydesc", //
                "numberofemployees_trend_reliabilitycode", //
                "numberofemployees_trend_timeperiod_desc", //
                "numberofemployees_trend_timeperiod_code", //
                "numberofemployees_trend_val", //
                "numberofemployees_val", //
                "primaryaddr_geographicalprecision_desc", //
                "primaryaddr_geographicalprecision_code", //
                "primaryaddr_ismanufacturinglocation", //
                "primaryaddr_latitude", //
                "primaryaddr_locationownership_desc", //
                "primaryaddr_locationownership_code", //
                "primaryaddr_longitude", //
                "primaryaddr_premisesarea_measurement", //
                "primaryaddr_premisesarea_reliabilitydesc", //
                "primaryaddr_premisesarea_reliabilitycode", //
                "primaryaddr_premisesarea_unitdesc", //
                "primaryaddr_premisesarea_unitcode", //
                "primaryaddr_statisticalarea_economicareaofinfluencecode", //
                "registeredaddr_country_isoalpha2code", //
                "registeredaddr_country_name", //
                "registeredaddr_county_name", //
                "registeredaddr_addrlocality_name", //
                "registeredaddr_addrregion_abbreviatedname", //
                "registeredaddr_addrregion_name", //
                "registeredaddr_language_desc", //
                "registeredaddr_language_code", //
                "registeredaddr_minortownname", //
                "registeredaddr_postalcode", //
                "registeredaddr_postalcodeposition_desc", //
                "registeredaddr_postalcodeposition_code", //
                "registeredaddr_postofficebox_postofficeboxnumber", //
                "registeredaddr_postofficebox_typedesc", //
                "registeredaddr_postofficebox_typecode", //
                "registeredaddr_street_line1", //
                "registeredaddr_street_line2", //
                "registeredaddr_street_line3", //
                "registeredaddr_street_line4", //
                "registeredaddr_streetname", //
                "registeredaddr_streetnumber", //
                "registered_legalform_desc", //
                "registered_legalform_code", //
                "registered_legalform_registrationstatus_desc", //
                "registered_legalform_registrationstatus_code", //
                "registeredname", //
                "startdate" //
        ).map(Column::new).collect(Collectors.toList());
        ColumnSelection cs = new ColumnSelection();
        cs.setColumns(columns);
        return cs;
    }

}
