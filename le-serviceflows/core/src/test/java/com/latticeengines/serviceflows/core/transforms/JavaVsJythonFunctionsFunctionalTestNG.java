package com.latticeengines.serviceflows.core.transforms;

import static org.testng.Assert.assertEquals;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;
import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.context.web.WebAppConfiguration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.jython.JythonEngine;
import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.TransformId;
import com.latticeengines.transform.exposed.TransformRetriever;

@WebAppConfiguration
@ContextConfiguration(locations = { "classpath:test-serviceflows-context.xml" })
public class JavaVsJythonFunctionsFunctionalTestNG extends AbstractTestNGSpringContextTests {

    private static final Boolean DEBUGGING_OUTPUT = Boolean.FALSE;
    private static final Boolean EXECUTION_DETAILS_OUTPUT = Boolean.TRUE;
    private static final Boolean WRITE_FLOATING_POINT_DIFFERENCES = Boolean.FALSE;
    private static final double FLOATING_POINT_PRECISION = 1.0e-15;

    private JythonEngine engine = new JythonEngine(null);

    @Autowired
    private TransformRetriever transformRetriever;

    @DataProvider(name = "functions")
    public Object[][] getFunctions() {
        return new Object[][] { //
        new Object[] { "title_length", "length", Integer.class, new String[] { "xyz" }, 3 } //

        };
    }

    @Test(groups = "functional", dataProvider = "functions")
    public void testFunctions(String moduleName, //
            String functionName, //
            Class<?> returnType, //
            String[] params, //
            Object expectedResult) {
        Object result = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                moduleName, functionName, params, returnType);
        assertEquals(result, expectedResult);
    }

    @Test(groups = "functional")
    public void testLegacyFunctionsWithDatasets() throws Exception {
        String testDataDSAPACMKTO = "src/test/resources/testdata_legacy_DS_APAC_MKTO.csv";

        String outputFileName = "delta_floating_point.csv";
        CSVPrinter output = null;
        CSVFormat outputCSVFormat = CSVFormat.DEFAULT.withRecordSeparator("\n");

        int numberOfErrors = 0;
        int maxNumberOfErrors = 5;

        try (InputStreamReader readerTestDataDSAPACMKTO = new InputStreamReader(new BOMInputStream(new FileInputStream(
                testDataDSAPACMKTO))); FileWriter writer = new FileWriter(outputFileName)) {

            if (DEBUGGING_OUTPUT && WRITE_FLOATING_POINT_DIFFERENCES) {
                output = new CSVPrinter(writer, outputCSVFormat);
                Object[] FILE_HEADER = { "delta" };
                output.printRecord(FILE_HEADER);
            }

            long totalReadTime = 0;
            long totalAttTime = 0;
            long totalFcn1Time = 0;
            long lastLoopEndTime = 0;

            long i = 0;
            long nValidAlexaRelatedLinks = 0;
            long nValidModelAction = 0;
            long nValidJobsTrend = 0;
            long nValidFundingStage = 0;

            for (CSVRecord record : CSVFormat.EXCEL.withHeader().parse(readerTestDataDSAPACMKTO)) {

                if(i++ % 200 == 1)
                    System.out.println(i);

                long startReadTime = System.currentTimeMillis();
                if (lastLoopEndTime > 0) {
                    totalReadTime += startReadTime - lastLoopEndTime;
                }

                String leadID = record.get("LeadID");
                String email = record.get("Email");
                String emailDomain = record.get("EmailDomain");
                String firstName = record.get("FirstName");
                String lastName = record.get("LastName");
                String title = record.get("Title");
                String phone = record.get("Phone");
                String company = record.get("Company");
                String industry = record.get("Industry");
                String alexaOnlineSince = record.get("AlexaOnlineSince");
                String alexaRelatedLinks = record.get("AlexaRelatedLinks");
                String fundingStage = record.get("FundingStage");
                String jobsTrendString = record.get("JobsTrendString");
                String modelAction = record.get("ModelAction");
                String dsCompanyEntropy = record.get("DS_CompanyName_Entropy");
                String dsCompanyLength = record.get("DS_CompanyName_Length");
                String dsDomainLength = record.get("DS_Domain_Length");
                String dsFirstLastName = record.get("DS_FirstName_SameAs_LastName");
                String dsPDFundingStage = record.get("DS_PD_FundingStage_Ordered");
                String dsIndustryGroup = record.get("DS_Industry_Group");
                String dsPDJobsTrendString = record.get("DS_PD_JobsTrendString_Ordered");
                String dsPDModelAction = record.get("DS_PD_ModelAction_Ordered");
                String dsPhoneEntropy = record.get("DS_Phone_Entropy");
                String dsPDAlexaRelatedLinks = record.get("DS_PD_Alexa_RelatedLinks_Count");
                String dsSpamIndicator = record.get("DS_SpamIndicator");
                String dsTitleIsAcademic = record.get("DS_Title_IsAcademic");
                String dsTitleIsTechRelated = record.get("DS_Title_IsTechRelated");
                String dsTitleLength = record.get("DS_Title_Length");
                String dsTitleLevel = record.get("DS_Title_Level");
                String emailDomainIsPublic = record.get("Email_Domain_IsPublic");
                String alexaMonthsSinceOnline = record.get("Alexa_MonthsSinceOnline");

                long attributeTime = System.currentTimeMillis();
                totalAttTime = attributeTime - startReadTime;

                String functionName = "std_visidb_ds_companyname_entropy";
                Object resultCompanyEntropy = engine.invoke(
                        "com.latticeengines.serviceflows.core.transforms", //
                        functionName, functionName,
                        new String[] { company }, Double.class);

                long fc1Time = System.currentTimeMillis();
                totalFcn1Time += fc1Time - attributeTime;

                Boolean passesCompanyEntropy = passesDoubleValues(resultCompanyEntropy, dsCompanyEntropy);
                assertEquals(passesCompanyEntropy, Boolean.TRUE);
                Object value = this.applyJavaTransform(functionName, new String[] { company });
                if(value != null && resultCompanyEntropy != null)
                    Assert.assertTrue(Math.abs( (double)resultCompanyEntropy - (double)value) < 0.000001 );
                else if(value != null && resultCompanyEntropy == null)
                    Assert.assertTrue(false);
                else if(resultCompanyEntropy != null && value == null)
                    Assert.assertTrue(false);

                functionName = "std_length";
                Object resultTitleLength = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                        "std_length", "std_length", new String[] { title }, Integer.class);

                Boolean passesTitleLength = passesIntegerValues(resultTitleLength, dsTitleLength);
                assertEquals(passesTitleLength, Boolean.TRUE);
                value = this.applyJavaTransform(functionName, new String[] { title });
                assertEquals(resultTitleLength.toString(), value.toString());

                Object resultCompanyLength = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                        "std_length", "std_length", new String[] { company }, Integer.class);

                Boolean passesCompanyLength = passesIntegerValues(resultCompanyLength, dsCompanyLength);
                assertEquals(passesCompanyLength, Boolean.TRUE);

                functionName = "std_visidb_ds_pd_alexa_relatedlinks_count";
                Object resultAlexaRelatedLinks = engine.invoke(
                        "com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_pd_alexa_relatedlinks_count", "std_visidb_ds_pd_alexa_relatedlinks_count",
                        new String[] { alexaRelatedLinks }, Integer.class);
                value = this.applyJavaTransform(functionName, new String[] { alexaRelatedLinks });
                assertEquals(checkForEquality(resultAlexaRelatedLinks, value), Boolean.TRUE);

                Boolean passesAlexaRelatedLinks = Boolean.FALSE;
                if (emailDomain.equals("") || emailDomainIsPublic.equals("1")) {
                    // The original function is buggy; there is no way to
                    // completely replicate it currently
                    passesAlexaRelatedLinks = Boolean.TRUE;
                    if (alexaRelatedLinks.equals("")) {
                        nValidAlexaRelatedLinks += 1;
                    }
                } else {
                    passesAlexaRelatedLinks = passesIntegerValues(resultAlexaRelatedLinks, dsPDAlexaRelatedLinks);
                    nValidAlexaRelatedLinks += 1;
                }

                if (!passesAlexaRelatedLinks) {
                    System.out.println(String.format("ID=%s, Src=%s, Orig=%s, ED=%s", leadID, alexaRelatedLinks,
                            dsPDAlexaRelatedLinks, emailDomainIsPublic));
                }
                assertEquals(passesAlexaRelatedLinks, Boolean.TRUE);

                functionName = "std_visidb_ds_pd_modelaction_ordered";
                Object resultModelAction = engine.invoke(
                        "com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_pd_modelaction_ordered", "std_visidb_ds_pd_modelaction_ordered",
                        new String[] { modelAction }, Integer.class);
                value = this.applyJavaTransform(functionName, new String[] { modelAction });
                assertEquals(checkForEquality(resultModelAction, value), Boolean.TRUE);

                Boolean passesModelAction = Boolean.FALSE;
                if (emailDomain.equals("") || emailDomainIsPublic.equals("1")) {
                    // The original function is buggy; there is no way to
                    // completely replicate it currently
                    passesModelAction = Boolean.TRUE;
                    if (modelAction.equals("")) {
                        nValidModelAction += 1;
                    }
                } else {
                    passesModelAction = passesIntegerValues(resultModelAction, dsPDModelAction);
                    nValidModelAction += 1;
                }

                if (!passesModelAction) {
                    System.out.println(String.format("ID=%s, Src=%s, Orig=%s, New=%d", leadID, modelAction,
                            dsPDModelAction, (int) resultModelAction));
                }
                assertEquals(passesModelAction, Boolean.TRUE);

                functionName = "std_visidb_ds_pd_jobstrendstring_ordered";
                Object resultJobsTrend = engine.invoke(
                        "com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_pd_jobstrendstring_ordered", "std_visidb_ds_pd_jobstrendstring_ordered",
                        new String[] { jobsTrendString }, Integer.class);
                value = this.applyJavaTransform(functionName, new String[] { jobsTrendString });
                assertEquals(checkForEquality(resultJobsTrend, value), Boolean.TRUE);

                Boolean passesJobsTrend = Boolean.FALSE;
                if (emailDomain.equals("") || emailDomainIsPublic.equals("1")) {
                    // The original function is buggy; there is no way to
                    // completely replicate it currently
                    passesJobsTrend = Boolean.TRUE;
                    if (jobsTrendString.equals("")) {
                        nValidJobsTrend += 1;
                    }
                } else {
                    passesJobsTrend = passesIntegerValues(resultJobsTrend, dsPDJobsTrendString);
                    nValidJobsTrend += 1;
                }

                if (!passesJobsTrend) {
                    System.out.println(String.format("ID=%s, Src=%s, Orig=%s, New=%d", leadID, jobsTrendString,
                            dsPDJobsTrendString, (int) resultJobsTrend));
                }
                assertEquals(passesJobsTrend, Boolean.TRUE);

                functionName = "std_visidb_ds_pd_fundingstage_ordered";
                Object resultFundingStage = engine.invoke(
                        "com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_pd_fundingstage_ordered", "std_visidb_ds_pd_fundingstage_ordered",
                        new String[] { fundingStage }, Integer.class);
                value = this.applyJavaTransform(functionName, new String[] { fundingStage });
                assertEquals(checkForEquality(resultFundingStage, value), Boolean.TRUE);

                Boolean passesFundingStage = Boolean.FALSE;
                if (emailDomain.equals("") || emailDomainIsPublic.equals("1")) {
                    // The original function is buggy; there is no way to
                    // completely replicate it currently
                    passesFundingStage = Boolean.TRUE;
                    if (fundingStage.equals("")) {
                        nValidFundingStage += 1;
                    }
                } else {
                    passesFundingStage = passesIntegerValues(resultFundingStage, dsPDFundingStage);
                    nValidFundingStage += 1;
                }

                if (!passesFundingStage) {
                    System.out.println(String.format("ID=%s, Src=%s, Orig=%s, New=%d", leadID, fundingStage,
                            dsPDFundingStage, (int) resultFundingStage));
                }
                assertEquals(passesFundingStage, Boolean.TRUE);

                functionName = "std_length";
                Object resultDomainLength = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                        "std_length", "std_length", new String[] { emailDomain }, Integer.class);
                value = this.applyJavaTransform(functionName, new String[] { emailDomain });
                assertEquals(checkForEquality(resultDomainLength, value), Boolean.TRUE);

                Boolean passesDomainLength = passesIntegerValues(resultDomainLength, dsDomainLength);
                assertEquals(passesDomainLength, Boolean.TRUE);

                functionName = "std_entropy";
                Object resultPhoneEntropy = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                        "std_entropy", "std_entropy", new String[] { phone }, Double.class);
                value = this.applyJavaTransform(functionName, new String[] { phone });
                if(value != null && resultPhoneEntropy != null)
                    Assert.assertTrue(Math.abs( (double)resultPhoneEntropy - (double)value) < 0.000001 );
                else if(value != null && resultPhoneEntropy == null)
                    Assert.assertTrue(false);
                else if(resultPhoneEntropy != null && value == null)
                    Assert.assertTrue(false);

                Boolean passesPhoneEntropy = passesDoubleValues(resultPhoneEntropy, dsPhoneEntropy);
                assertEquals(passesPhoneEntropy, Boolean.TRUE);

                functionName = "std_visidb_alexa_monthssinceonline";
                Object resultMonthsSinceOnline = engine.invoke(
                        "com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_alexa_monthssinceonline", "std_visidb_alexa_monthssinceonline",
                        new String[] { alexaOnlineSince }, Integer.class);
                value = this.applyJavaTransform(functionName, new String[] { alexaOnlineSince });
                if(value != null && resultMonthsSinceOnline != null)
                    Assert.assertTrue(Math.abs( (int)resultMonthsSinceOnline - (int)value) < 2 );
                else if(value != null && resultMonthsSinceOnline == null)
                    Assert.assertTrue(false);
                else if(resultMonthsSinceOnline != null && value == null)
                    Assert.assertTrue(false);

                Boolean passesMonthsSinceOnline = passesIntegerValues(resultMonthsSinceOnline, alexaMonthsSinceOnline);
                //
                // This function can only be properly tested when the dataset is
                // produced on the same day as the test
                // is run. For now, just check that it executes.
                //
                // if (!passesMonthsSinceOnline) {
                // System.out.println(String.format("ID=%s, Src=%s, Orig=%s,
                // New=%d", leadID, alexaOnlineSince,
                // alexaMonthsSinceOnline, (int) resultMonthsSinceOnline));
                // }
                // assertEquals(passesMonthsSinceOnline, Boolean.TRUE);

                functionName = "std_visidb_ds_firstname_sameas_lastname";
                Object resultFirstLastName = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_firstname_sameas_lastname", "std_visidb_ds_firstname_sameas_lastname", //
                        new String[] { firstName, lastName }, Boolean.class);
                value = this.applyJavaTransform(functionName, new String[] { firstName, lastName });
                assertEquals(checkForEquality(resultFirstLastName, value), Boolean.TRUE);

                Boolean passesFirstLastName = passesBooleanValues(resultFirstLastName, dsFirstLastName);
                assertEquals(passesFirstLastName, Boolean.TRUE);

                functionName = "std_visidb_ds_title_level";
                Object resultTitleLevel = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_title_level", "std_visidb_ds_title_level", //
                        new String[] { title }, Integer.class);
                value = this.applyJavaTransform(functionName, new String[] { title });
                if(!checkForEquality(resultTitleLevel, value)) {
                    System.out.println("1: " + resultTitleLevel + " " + value + " " + title);
                    numberOfErrors++;
                }

                Boolean passesTitleLevel = passesIntegerValues(resultTitleLevel, dsTitleLevel);
                assertEquals(passesTitleLevel, Boolean.TRUE);

                functionName = "std_visidb_ds_title_istechrelated";
                Object resultTitleIsTechRelated = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_title_istechrelated", "std_visidb_ds_title_istechrelated", //
                        new String[] { title }, Boolean.class);
                value = this.applyJavaTransform(functionName, new String[] { title });
                if(!checkForEquality(resultTitleIsTechRelated, value)) {
                    System.out.println("2: " + resultTitleIsTechRelated + " " + value + " " + title);
                    numberOfErrors++;
                }

                Boolean passesTitleIsTechRelated = passesBooleanValues(resultTitleIsTechRelated, dsTitleIsTechRelated);
                assertEquals(passesTitleIsTechRelated, Boolean.TRUE);

                functionName = "std_visidb_ds_title_isacademic";
                Object resultTitleIsAcademic = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_title_isacademic", "std_visidb_ds_title_isacademic", //
                        new String[] { title }, Boolean.class);
                value = this.applyJavaTransform(functionName, new String[] { title });
                if(!checkForEquality(resultTitleIsAcademic, value)) {
                    System.out.println("3: " + resultTitleIsAcademic + " " + value + " " + title);
                    numberOfErrors++;
                }

                Boolean passesTitleIsAcademic = passesBooleanValues(resultTitleIsAcademic, dsTitleIsAcademic);
                assertEquals(passesTitleIsAcademic, Boolean.TRUE);

                functionName = "std_visidb_ds_industry_group";
                Object resultIndustryGroup = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_industry_group", "std_visidb_ds_industry_group", //
                        new String[] { industry }, String.class);
                value = this.applyJavaTransform(functionName, new String[] { industry });
                if(!checkForEquality(resultIndustryGroup, value)) {
                    System.out.println("4: " + resultIndustryGroup + " " + value + " " + industry);
                    numberOfErrors++;
                }

                Boolean passesIndustryGroup = passesStringValues(resultIndustryGroup, dsIndustryGroup);
                assertEquals(passesIndustryGroup, Boolean.TRUE);

                functionName = "std_visidb_ds_spamindicator";
                Object resultSpamIndicator = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_spamindicator", "std_visidb_ds_spamindicator", //
                        new String[] { firstName, lastName, title, phone, company }, String.class);
                value = this.applyJavaTransform(functionName, new String[] { firstName, lastName, title, phone, company });
                if(!checkForEquality(resultSpamIndicator, value)) {
                    System.out.println("5: " + resultSpamIndicator + " " + value + " " + firstName + " " + lastName + " " + title + " " + phone + " " + company);
                    numberOfErrors++;
                }

                Boolean passesSpamIndicator = passesIntegerValues(resultSpamIndicator, dsSpamIndicator);
                assertEquals(passesSpamIndicator, Boolean.TRUE);

                if (DEBUGGING_OUTPUT) {
                    if (WRITE_FLOATING_POINT_DIFFERENCES) {
                        List<String> outputRecord = new ArrayList<>();
                        if (!record.get("DS_CompanyName_Entropy").equals("")) {
                            if (Math.abs(Double.parseDouble(record.get("DS_CompanyName_Entropy"))
                                    - (double) resultCompanyEntropy) > 1.0e-16) {
                                System.out.println(String.format("Name=%s, Orig=%f, New=%f", company,
                                        Double.parseDouble(record.get("DS_CompanyName_Entropy")),
                                        (double) resultCompanyEntropy));
                            }
                            outputRecord.add(String.valueOf(Double.parseDouble(record.get("DS_CompanyName_Entropy"))
                                    - (double) resultCompanyEntropy));
                            output.printRecord(outputRecord);
                        }
                    }
                    System.out.println(String.format("Name=%s, Orig=%f, New=%f", company,
                            Double.parseDouble(record.get("DS_CompanyName_Entropy")), (double) resultCompanyEntropy));
                }

                lastLoopEndTime = System.currentTimeMillis();
            }
            if (EXECUTION_DETAILS_OUTPUT) {
                System.out.println(String.format("Number of rows processed: %d", i));
                System.out.println(String.format("Avg Read Time: %f", (totalReadTime) / ((double) i - 1)));
                System.out.println(String.format("Avg Att Time: %f", ((double) totalAttTime) / ((double) i)));
                System.out.println(String.format("Avg Function (1) Time: %f", ((double) totalFcn1Time) / ((double) i)));
                System.out.println(String.format("Valid AlexaRelatedLinks Calculation: %f",
                        (double) nValidAlexaRelatedLinks / (double) i));
                System.out.println(String.format("Valid ModelAction Calculation: %f", (double) nValidModelAction
                        / (double) i));
                System.out.println(String.format("Valid JobsTrend Calculation: %f", (double) nValidJobsTrend
                        / (double) i));
                System.out.println(String.format("Valid FundingStage Calculation: %f", (double) nValidFundingStage
                        / (double) i));
            }

            Assert.assertTrue("Java vs Jython transformation test failed. Unacceptable number of mismatches. ", numberOfErrors < maxNumberOfErrors);
        }
    }

    private Object applyJavaTransform(String functionName, String[] values) {
        String modelPath = "";
        String buildVersion = null;
        TransformId id = new TransformId(modelPath, functionName, buildVersion);
        RealTimeTransform transform = transformRetriever.getTransform(id);

        Map<String, Object> recordAsMap = new HashMap<>();
        if(values.length == 1)
            recordAsMap.put(functionName, values[0]);
        else {
            int i = 1;
            for(String value:values)
                recordAsMap.put("column" + i++, value);
        }
        Map<String, Object> argumentsAsMap = new HashMap<>();
        if(values.length == 1)
            argumentsAsMap.put("column", functionName);
        else {
            int i = 1;
            for(String value:values)
                argumentsAsMap.put("column" + i++, "column" + (i - 1));
        }
        return transform.transform(argumentsAsMap, recordAsMap);
    }

    private Boolean checkForEquality(Object value1, Object value2) {
        if(value1 == null && value2 == null)
            return true;
        else {
            try {
                if(value1.toString().equals(value2.toString()))
                   return value1.toString().equals(value2.toString());
                else {
                    System.out.println(value1 + " " + value2);
                    return value1.toString().equals(value2.toString());
                }
            } catch(NullPointerException npe) {
                System.out.println(value1 + " " + value2);
                return false;
            }
        }
    }

    private Boolean passesDoubleValues(Object calc, String reference) {
        Boolean passes = Boolean.FALSE;
        if (calc == null) {
            passes = (reference.equals(""));
        } else if (reference.equals("")) {
            passes = Boolean.FALSE;
        } else {
            passes = (Math.abs(Double.parseDouble(reference) - (double) calc) < FLOATING_POINT_PRECISION);
        }
        return passes;
    }

    private Boolean passesIntegerValues(Object calc, String reference) {
        Boolean passes = Boolean.FALSE;
        if (calc == null) {
            passes = (reference.equals(""));
        } else if (reference.equals("")) {
            passes = Boolean.FALSE;
        } else {
            passes = (Integer.parseInt(reference) == (int) calc);
        }
        return passes;
    }

    private Boolean passesBooleanValues(Object calc, String reference) {
        if (reference.equals("1")) {
            reference = "true";
        } else if (reference.equals("0")) {
            reference = "false";
        }
        Boolean passes = Boolean.FALSE;
        if (calc == null) {
            passes = (reference.equals(""));
        } else if (reference.equals("")) {
            passes = Boolean.FALSE;
        } else {
            passes = (Boolean.parseBoolean(reference) == (boolean) calc);
        }
        return passes;
    }

    private Boolean passesStringValues(Object calc, String reference) {
        Boolean passes = Boolean.FALSE;
        if (calc == null) {
            passes = (reference.equals(""));
        } else if (reference.equals("")) {
            passes = Boolean.FALSE;
        } else {
            passes = (reference.equals(calc));
        }
        return passes;
    }
}
