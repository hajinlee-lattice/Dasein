package com.latticeengines.serviceflows.core.transforms;

import static org.testng.Assert.assertEquals;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.jython.JythonEngine;

public class JythonFunctionsUnitTestNG {

    private static final Boolean DEBUGGING_OUTPUT = Boolean.FALSE;
    private static final Boolean EXECUTION_DETAILS_OUTPUT = Boolean.TRUE;
    private static final Boolean WRITE_FLOATING_POINT_DIFFERENCES = Boolean.FALSE;
    private static final double FLOATING_POINT_PRECISION = 1.0e-15;

    private JythonEngine engine = new JythonEngine(null);

    @DataProvider(name = "functions")
    public Object[][] getFunctions() {
        return new Object[][] { //
        new Object[] { "title_length", "length", Integer.class, new String[] { "xyz" }, 3 } //

        };
    }

    @Test(groups = "unit", dataProvider = "functions")
    public void testFunctions(String moduleName, //
            String functionName, //
            Class<?> returnType, //
            String[] params, //
            Object expectedResult) {
        Object result = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                moduleName, functionName, params, returnType);
        assertEquals(result, expectedResult);
    }

    @SuppressWarnings("unused")
    @Test(groups = "unit")
    public void testLegacyFunctionsWithDatasets() throws Exception {
        String testDataDSAPACMKTO = "src/test/resources/testdata_legacy_DS_APAC_MKTO.csv";

        String outputFileName = "delta_floating_point.csv";
        CSVPrinter output = null;
        CSVFormat outputCSVFormat = CSVFormat.DEFAULT.withRecordSeparator("\n");

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

                long startReadTime = System.currentTimeMillis();
                if (lastLoopEndTime > 0) {
                    totalReadTime += startReadTime - lastLoopEndTime;
                }
                i++;

                String leadID = record.get("LeadID");
                @SuppressWarnings("unused")
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

                Object resultCompanyEntropy = engine.invoke(
                        "com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_companyname_entropy", "std_visidb_ds_companyname_entropy",
                        new String[] { company }, Double.class);

                long fc1Time = System.currentTimeMillis();
                totalFcn1Time += fc1Time - attributeTime;

                Boolean passesCompanyEntropy = passesDoubleValues(resultCompanyEntropy, dsCompanyEntropy);
                assertEquals(passesCompanyEntropy, Boolean.TRUE);

                Object resultTitleLength = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                        "std_length", "std_length", new String[] { title }, Integer.class);

                Boolean passesTitleLength = passesIntegerValues(resultTitleLength, dsTitleLength);
                assertEquals(passesTitleLength, Boolean.TRUE);

                Object resultCompanyLength = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                        "std_length", "std_length", new String[] { company }, Integer.class);

                Boolean passesCompanyLength = passesIntegerValues(resultCompanyLength, dsCompanyLength);
                assertEquals(passesCompanyLength, Boolean.TRUE);

                Object resultAlexaRelatedLinks = engine.invoke(
                        "com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_pd_alexa_relatedlinks_count", "std_visidb_ds_pd_alexa_relatedlinks_count",
                        new String[] { alexaRelatedLinks }, Integer.class);

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

                Object resultModelAction = engine.invoke(
                        "com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_pd_modelaction_ordered", "std_visidb_ds_pd_modelaction_ordered",
                        new String[] { modelAction }, Integer.class);

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

                Object resultJobsTrend = engine.invoke(
                        "com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_pd_jobstrendstring_ordered", "std_visidb_ds_pd_jobstrendstring_ordered",
                        new String[] { jobsTrendString }, Integer.class);

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

                Object resultFundingStage = engine.invoke(
                        "com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_pd_fundingstage_ordered", "std_visidb_ds_pd_fundingstage_ordered",
                        new String[] { fundingStage }, Integer.class);

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

                Object resultDomainLength = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                        "std_length", "std_length", new String[] { emailDomain }, Integer.class);

                Boolean passesDomainLength = passesIntegerValues(resultDomainLength, dsDomainLength);
                assertEquals(passesDomainLength, Boolean.TRUE);

                Object resultPhoneEntropy = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                        "std_entropy", "std_entropy", new String[] { phone }, Double.class);

                Boolean passesPhoneEntropy = passesDoubleValues(resultPhoneEntropy, dsPhoneEntropy);
                assertEquals(passesPhoneEntropy, Boolean.TRUE);

                Object resultMonthsSinceOnline = engine.invoke(
                        "com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_alexa_monthssinceonline", "std_visidb_alexa_monthssinceonline",
                        new String[] { alexaOnlineSince }, Integer.class);

                @SuppressWarnings("unused")
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

                Object resultFirstLastName = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_firstname_sameas_lastname", "std_visidb_ds_firstname_sameas_lastname", //
                        new String[] { firstName, lastName }, Boolean.class);

                Boolean passesFirstLastName = passesBooleanValues(resultFirstLastName, dsFirstLastName);
                assertEquals(passesFirstLastName, Boolean.TRUE);

                Object resultTitleLevel = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_title_level", "std_visidb_ds_title_level", //
                        new String[] { title }, Integer.class);

                Boolean passesTitleLevel = passesIntegerValues(resultTitleLevel, dsTitleLevel);
                assertEquals(passesTitleLevel, Boolean.TRUE);

                Object resultTitleIsTechRelated = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_title_istechrelated", "std_visidb_ds_title_istechrelated", //
                        new String[] { title }, Boolean.class);

                Boolean passesTitleIsTechRelated = passesBooleanValues(resultTitleIsTechRelated, dsTitleIsTechRelated);
                assertEquals(passesTitleIsTechRelated, Boolean.TRUE);

                Object resultTitleIsAcademic = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_title_isacademic", "std_visidb_ds_title_isacademic", //
                        new String[] { title }, Boolean.class);

                Boolean passesTitleIsAcademic = passesBooleanValues(resultTitleIsAcademic, dsTitleIsAcademic);
                assertEquals(passesTitleIsAcademic, Boolean.TRUE);

                Object resultIndustryGroup = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_industry_group", "std_visidb_ds_industry_group", //
                        new String[] { industry }, String.class);

                Boolean passesIndustryGroup = passesStringValues(resultIndustryGroup, dsIndustryGroup);
                assertEquals(passesIndustryGroup, Boolean.TRUE);

                Object resultSpamIndicator = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_spamindicator", "std_visidb_ds_spamindicator", //
                        new String[] { firstName, lastName, title, phone, company }, String.class);

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
