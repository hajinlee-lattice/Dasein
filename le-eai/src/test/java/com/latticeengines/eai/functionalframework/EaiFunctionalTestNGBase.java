package com.latticeengines.eai.functionalframework;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.net.URL;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.yarn.client.YarnClient;
import org.testng.annotations.BeforeClass;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.Field;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-eai-context.xml" })
public class EaiFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    protected static final Log log = LogFactory.getLog(EaiFunctionalTestNGBase.class);

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private YarnClient defaultYarnClient;

    @Autowired
    private MetadataService metadataService;

    protected DataPlatformFunctionalTestNGBase platformTestBase;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setupRunEnvironment() throws Exception {
        platformTestBase = new DataPlatformFunctionalTestNGBase(yarnConfiguration);
        platformTestBase.setYarnClient(defaultYarnClient);
    }

    protected Table createMarketoActivity() {
        Table table = new Table();
        table.setName("Activity");
        Attribute id = new Attribute();
        id.setName("id");
        id.setDisplayName("Id");
        id.setLogicalDataType("String");
        Attribute leadId = new Attribute();
        leadId.setName("leadId");
        leadId.setDisplayName("Lead Id");
        leadId.setLogicalDataType("Int");
        Attribute activityDate = new Attribute();
        activityDate.setName("activityDate");
        activityDate.setDisplayName("Activity Date");
        activityDate.setLogicalDataType("Timestamp");
        Attribute activityTypeId = new Attribute();
        activityTypeId.setName("activityTypeId");
        activityTypeId.setDisplayName("Activity Type Id");
        activityTypeId.setLogicalDataType("Int");
        table.addAttribute(id);
        table.addAttribute(leadId);
        table.addAttribute(activityDate);
        table.addAttribute(activityTypeId);
        return table;
    }

    protected Table createMarketoActivityType() {
        Table table = new Table();
        table.setName("ActivityType");
        Attribute id = new Attribute();
        id.setName("id");
        id.setDisplayName("Id");
        id.setLogicalDataType("String");

        Attribute name = new Attribute();
        name.setName("name");
        name.setDisplayName("Name");
        name.setLogicalDataType("String");

        Attribute description = new Attribute();
        description.setName("description");
        description.setDisplayName("Description");
        description.setLogicalDataType("String");

        Attribute attributes = new Attribute();
        attributes.setName("attributes");
        attributes.setDisplayName("Attributes");
        attributes.setLogicalDataType("String");

        table.addAttribute(id);
        table.addAttribute(name);
        table.addAttribute(description);
        table.addAttribute(attributes);
        return table;
    }

    protected Table createMarketoLead() {
        Table table = new Table();
        table.setName("Lead");

        Attribute id = new Attribute();
        id.setName("id");
        Attribute anonymousIP = new Attribute();
        anonymousIP.setName("anonymousIP");
        Attribute inferredCompany = new Attribute();
        inferredCompany.setName("inferredCompany");
        Attribute inferredCountry = new Attribute();
        inferredCountry.setName("inferredCountry");
        Attribute title = new Attribute();
        title.setName("title");
        Attribute department = new Attribute();
        department.setName("department");
        Attribute unsubscribed = new Attribute();
        unsubscribed.setName("unsubscribed");
        Attribute unsubscribedReason = new Attribute();
        unsubscribedReason.setName("unsubscribedReason");
        Attribute doNotCall = new Attribute();
        doNotCall.setName("doNotCall");
        Attribute country = new Attribute();
        country.setName("country");
        Attribute website = new Attribute();
        website.setName("website");
        Attribute email = new Attribute();
        email.setName("email");
        Attribute leadStatus = new Attribute();
        leadStatus.setName("leadStatus");
        Attribute company = new Attribute();
        company.setName("company");
        Attribute leadSource = new Attribute();
        leadSource.setName("leadSource");
        Attribute industry = new Attribute();
        industry.setName("industry");
        Attribute annualRevenue = new Attribute();
        annualRevenue.setName("annualRevenue");
        Attribute numEmployees = new Attribute();
        numEmployees.setName("numberOfEmployees");
        Attribute doNotCallReason = new Attribute();
        doNotCallReason.setName("doNotCallReason");
        Attribute sicCode = new Attribute();
        sicCode.setName("sicCode");
        Attribute phone = new Attribute();
        phone.setName("phone");
        Attribute facebookReferredEnrollments = new Attribute();
        facebookReferredEnrollments.setName("facebookReferredEnrollments");
        Attribute facebookReferredVisits = new Attribute();
        facebookReferredVisits.setName("facebookReferredVisits");

        table.addAttribute(id);
        table.addAttribute(anonymousIP);
        table.addAttribute(inferredCompany);
        table.addAttribute(inferredCountry);
        table.addAttribute(title);
        table.addAttribute(department);
        table.addAttribute(unsubscribed);
        table.addAttribute(unsubscribedReason);
        table.addAttribute(doNotCall);
        table.addAttribute(country);
        table.addAttribute(website);
        table.addAttribute(email);
        table.addAttribute(leadStatus);
        table.addAttribute(company);
        table.addAttribute(leadSource);
        table.addAttribute(industry);
        table.addAttribute(annualRevenue);
        table.addAttribute(numEmployees);
        table.addAttribute(doNotCallReason);
        table.addAttribute(sicCode);
        table.addAttribute(phone);
        table.addAttribute(facebookReferredEnrollments);
        table.addAttribute(facebookReferredVisits);

        return table;
    }

    protected Table createFile(URL inputUrl, String fileName) {
        String url = String.format("jdbc:relique:csv:%s", inputUrl.getPath());
        String driver = "org.relique.jdbc.csv.CsvDriver";
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.jdbcUrl(url).driverClass(driver).dbType("GenericJDBC");
        DbCreds creds = new DbCreds(builder);

        DataSchema schema = metadataService.createDataSchema(creds, fileName);

        Table file = new Table();
        file.setName(fileName);

        for (Field field : schema.getFields()) {
            Attribute attr = new Attribute();
            attr.setName(field.getName());
            file.addAttribute(attr);
        }

        return file;
    }

    protected void verifyAllDataNotNullWithNumRows(Configuration config, String targetDir, int expectedNumRows)
            throws Exception {
        List<String> avroFiles = HdfsUtils.getFilesByGlob(config, String.format("%s/*.avro", targetDir));

        int numRows = 0;
        for (String avroFile : avroFiles) {
            try (FileReader<GenericRecord> reader = AvroUtils.getAvroFileReader(config, new org.apache.hadoop.fs.Path(
                    avroFile))) {
                while (reader.hasNext()) {
                    GenericRecord record = reader.next();
                    Schema schema = record.getSchema();
                    for (org.apache.avro.Schema.Field field : schema.getFields()) {
                        assertNotNull(record.get(field.name()));
                    }
                    numRows++;
                }
            }
        }
        assertEquals(numRows, expectedNumRows);
    }

    protected List<String> getFilesFromHdfs(String table, String targetPath) throws Exception{
        return HdfsUtils.getFilesForDirRecursive(yarnConfiguration, targetPath + "/" + table,
                new HdfsFileFilter() {

            @Override
            public boolean accept(FileStatus file) {
                return file.getPath().getName().endsWith(".avro");
            }

        });
    }
}
