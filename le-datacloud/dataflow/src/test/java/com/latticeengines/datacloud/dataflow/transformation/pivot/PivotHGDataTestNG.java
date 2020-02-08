package com.latticeengines.datacloud.dataflow.transformation.pivot;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.transformation.config.source.PivotTransformerConfig;

public class PivotHGDataTestNG extends DataCloudDataFlowFunctionalTestNGBase {

    // This data set is to cover the 1:m mapping case
    private static final String DATACENTERSOLUTION_ONE = "CloudTechnologies_DataCenterSolutions_One";
    private static final String DATACENTERSOLUTION_ONE_ARGU = "{\"PivotKeyColumn\":\"HG_Category_1\",\"PivotValueColumn\":\"*\","
            + "\"TargetPivotKeys\":\"Virtualization: Server & Data Center,"
            + "CloudTechnologies_DataCenterSolutions_One," + "Data Archiving{{COMMA}} Back-Up & Recovery,"
            + "Data Management & Storage (Hardware)," + "Operating Systems & Computing Languages,"
            + "IT Infrastructure & Operations Management," + "System Analytics & Monitoring,"
            + "Database Management Software,"
            + "Disaster Recovery (DR),Security Information and Event Management (SIEM),"
            + "Reporting Software\",\"IsNull\":0}";

    // This data set is to cover the 1:m mapping case
    private static final String NETWORKCOMPUTING_ONE = "cloudTechnologies_networkcomputing_one";
    private static final String NETWORKCOMPUTING_ONE_ARGU = "{\"PivotKeyColumn\":\"HG_Category_1\",\"PivotValueColumn\":\"*\","
            + "\"TargetPivotKeys\":\"Network Computing,Network Management (Software),"
            + "Network Management (Hardware),Automated Process/Workflow Systems,"
            + "Middleware Software,Electronic Data Interchange (EDI)\",\"IsNull\":0}";

    // This data set is to cover the 1:1 mapping case
    private static final String BUSINESS_INTELLIGENCE = "CloudTechnologies_BusinessIntelligence";
    private static final String BUSINESS_INTELLIGENCE_ARGU = "{\"PivotKeyColumn\":\"HG_Category_1\",\"PivotValueColumn\":\"*\","
            + "\"TargetPivotKeys\":\"Business Intelligence\",\"IsNull\":0}";

    // This data set is to cover the exist case
    private static final String TECHIND_AMAZONREDSHIFT = "TechIndicator_AmazonRedshit";
    private static final String TECHIND_AMAZONREDSHIFT_ARGU = "{\"PivotKeyColumn\":\"Segment_Name\",\"PivotValueColumn\":\"Segment_Name\","
            + "\"TargetPivotKeys\":\"Amazon Redshift\",\"BooleanType\":\"YES_NO\",\"IsNull\":\"No\"}";

    // This data set is to cover the no exist case
    private static final String TECHIND_GOOGLE = "TechIndicator_Google";
    private static final String TECHIND_GOOGLE_ARGU = "{\"PivotKeyColumn\":\"Segment_Name\",\"PivotValueColumn\":\"Segment_Name\","
            + "\"TargetPivotKeys\":\"Google\",\"BooleanType\":\"YES_NO\",\"IsNull\":\"No\"}";

    // This is to cover the new technology case
    private static final String NEW_TECH = "CloudTechnologies_NewTechnologyDetected";

    private static final SimpleDateFormat FORMATTER = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");

    @Override
    protected String getFlowBeanName() {
        return PivotHGData.BEAN_NAME;
    }

    @Test(groups = "functional", enabled = true)
    public void testRunFlow() {
        TransformationFlowParameters parameters = prepareInput();
        executeDataFlow(parameters);
        verifyResult();
    }

    private TransformationFlowParameters prepareInput() {
        // construct a HG data source for the pivoted process later
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Domain", String.class), //
                Pair.of("Supplier_Name", String.class), //
                Pair.of("Segment_Name", String.class), //
                Pair.of("HG_Category_1", String.class), //
                Pair.of("Last_Verified_Date", Long.class), //
                Pair.of("LE_Last_Upload_Date", Long.class) //
        );

        Object[][] data = new Object[][] { //
                { "3m.com", "Amazon Web Services, Inc.", "Amazon Redshift", "Database Management Software",
                        dateStrToMillis("07/13/2019 17:00:00"), dateStrToMillis("12/20/2019 16:30:32") }, //
                { "3m.com", "IBM Corporation", "IBM WebSphere Transformation Extender",
                        "Electronic Data Interchange (EDI)", dateStrToMillis("05/09/2019 17:00:00"),
                        dateStrToMillis("12/20/2019 16:30:32") }, //
                { "3m.com", "Microsoft Corporation", "Microsoft SQL Server Analysis Services",
                        "System Analytics & Monitoring", dateStrToMillis("05/12/2019 17:00:00"),
                        dateStrToMillis("12/20/2019 16:30:32") }, //
                { "3m.com", "Cisco Systems, Inc.", "Meraki", "Network Management (Hardware)",
                        dateStrToMillis("06/12/2019 17:00:00"), dateStrToMillis("12/20/2019 16:30:32") }, //
                { "3m.com", "Datadog, Inc.", "Datadog", "IT Infrastructure & Operations Management",
                        dateStrToMillis("07/20/2019 16:30:32"), dateStrToMillis("12/20/2019 16:30:32") }, //
                { "3m.com", "Open Source", "Linux", "Operating Systems & Computing Languages",
                        dateStrToMillis("08/04/2019 17:00:00"), dateStrToMillis("12/20/2019 16:30:32") }, //
                { "3m.com", "HPE", "HPE Storage Systems", "Data Management & Storage (Hardware)",
                        dateStrToMillis("05/18/2019 17:00:00"), dateStrToMillis("12/20/2019 16:30:32") }, //
                { "3m.com", "Juniper Networks, Inc.", "Juniper Networks ISG-Series", "System Security Services",
                        dateStrToMillis("05/31/2019 17:00:00"), dateStrToMillis("12/20/2019 16:30:32") }, //
                { "3m.com", "Open Source", "Wireshark", "Network Management (Software)",
                        dateStrToMillis("08/27/2019 17:00:00"), dateStrToMillis("12/20/2019 16:30:32") }, //
                { "3m.com", "Oracle Corporation", "Oracle Endeca", "Business Intelligence",
                        dateStrToMillis("09/27/2019 17:00:00"), dateStrToMillis("12/20/2019 16:30:32") }, //
        };

        uploadDataToSharedAvroInput(data, fields);
        TransformationFlowParameters parameters = new TransformationFlowParameters();
        parameters.setBaseTables(Collections.singletonList(AVRO_INPUT));

        // Set up source columns
        List<SourceColumn> columns = new ArrayList<>();
        setupSourceColumns(columns);
        parameters.setColumns(columns);

        parameters.setTimestampField("timestamp");

        PivotTransformerConfig config = new PivotTransformerConfig();
        String[] joinFields = { "Domain" };
        config.setJoinFields(joinFields);
        config.setBeanName(PivotHGData.BEAN_NAME);
        parameters.setConfJson(JsonUtils.serialize(config));

        return parameters;
    }

    private long dateStrToMillis(String dateStr) {
        try {
            Date date = FORMATTER.parse(dateStr);
            return date.getTime();
        } catch (ParseException e) {
            System.out.println("Date conversion failed.." + e.getStackTrace());
        }

        return -1L;
    }

    private void setupSourceColumns(List<SourceColumn> columns) {
        SourceColumn datacentersolutions_one = new SourceColumn();
        datacentersolutions_one.setSourceColumnId(1L);
        datacentersolutions_one.setArguments(DATACENTERSOLUTION_ONE_ARGU);
        datacentersolutions_one.setBaseSource(AVRO_INPUT);
        datacentersolutions_one.setCalculation(SourceColumn.Calculation.PIVOT_COUNT);
        datacentersolutions_one.setColumnName(DATACENTERSOLUTION_ONE);
        datacentersolutions_one.setColumnType("INT");
        datacentersolutions_one.setGroupBy("Domain");
        datacentersolutions_one.setSourceName("HGDataPivoted");

        SourceColumn networkcomputing_one = new SourceColumn();
        networkcomputing_one.setSourceColumnId(2L);
        networkcomputing_one.setArguments(NETWORKCOMPUTING_ONE_ARGU);
        networkcomputing_one.setBaseSource(AVRO_INPUT);
        networkcomputing_one.setCalculation(SourceColumn.Calculation.PIVOT_COUNT);
        networkcomputing_one.setColumnName(NETWORKCOMPUTING_ONE);
        networkcomputing_one.setColumnType("INT");
        networkcomputing_one.setGroupBy("Domain");
        networkcomputing_one.setSourceName("HGDataPivoted");

        SourceColumn business_intelligence = new SourceColumn();
        business_intelligence.setSourceColumnId(3L);
        business_intelligence.setArguments(BUSINESS_INTELLIGENCE_ARGU);
        business_intelligence.setBaseSource(AVRO_INPUT);
        business_intelligence.setCalculation(SourceColumn.Calculation.PIVOT_COUNT);
        business_intelligence.setColumnName(BUSINESS_INTELLIGENCE);
        business_intelligence.setColumnType("INT");
        business_intelligence.setGroupBy("Domain");
        business_intelligence.setSourceName("HGDataPivoted");

        SourceColumn techind_amazonredshift = new SourceColumn();
        techind_amazonredshift.setSourceColumnId(4L);
        techind_amazonredshift.setArguments(TECHIND_AMAZONREDSHIFT_ARGU);
        techind_amazonredshift.setBaseSource(AVRO_INPUT);
        techind_amazonredshift.setCalculation(SourceColumn.Calculation.PIVOT_EXISTS);
        techind_amazonredshift.setColumnName(TECHIND_AMAZONREDSHIFT);
        techind_amazonredshift.setColumnType("NVARCHAR(3)");
        techind_amazonredshift.setGroupBy("Domain");
        techind_amazonredshift.setSourceName("HGDataPivoted");

        SourceColumn techind_google = new SourceColumn();
        techind_google.setSourceColumnId(5L);
        techind_google.setArguments(TECHIND_GOOGLE_ARGU);
        techind_google.setBaseSource(AVRO_INPUT);
        techind_google.setCalculation(SourceColumn.Calculation.PIVOT_EXISTS);
        techind_google.setColumnName(TECHIND_GOOGLE);
        techind_google.setColumnType("NVARCHAR(3)");
        techind_google.setGroupBy("Domain");
        techind_google.setSourceName("HGDataPivoted");

        SourceColumn new_tech = new SourceColumn();
        new_tech.setSourceColumnId(6L);
        new_tech.setBaseSource(AVRO_INPUT);
        new_tech.setCalculation(SourceColumn.Calculation.HGDATA_NEWTECH);
        new_tech.setColumnName(NEW_TECH);
        new_tech.setColumnType("NVARCHAR(MAX)");
        new_tech.setGroupBy("Domain");
        new_tech.setSourceName("HGDataPivoted");

        columns.add(datacentersolutions_one);
        columns.add(networkcomputing_one);
        columns.add(business_intelligence);
        columns.add(techind_amazonredshift);
        columns.add(techind_google);
        columns.add(new_tech);
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        for (GenericRecord record : records) {
            System.out.println(record);
        }
        Assert.assertEquals(records.size(), 1);
        GenericRecord record = records.get(0);
        // Based on the input, the grouped CloudTechnologies_DataCenterSolutions_One
        // for domain 3m.com should be 5
        Assert.assertEquals(record.get(DATACENTERSOLUTION_ONE), 5);

        // Based on the input, the grouped cloudTechnologies_networkcomputing_one
        // for domain 3m.com should be 3
        Assert.assertEquals(record.get(NETWORKCOMPUTING_ONE), 3);

        // Based on the input, CloudTechnologies_BusinessIntelligence for
        // domain 3m.com should be 1
        Assert.assertEquals(record.get(BUSINESS_INTELLIGENCE), 1);

        // Based on the input, TechIndicator_AmazonRedshit for 3m.com should be Yes
        Assert.assertEquals(record.get(TECHIND_AMAZONREDSHIFT).toString(), "Yes");

        // Based on the input, TechIndicator_Google for 3m.com should be No
        Assert.assertEquals(record.get(TECHIND_GOOGLE).toString(), "No");

        System.out.println("PivotHGDataTestNG has completed successfully");
    }
}
