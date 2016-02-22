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
    private static final Boolean EXECUTION_TIME_OUTPUT = Boolean.FALSE;
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

    @Test(groups = "unit")
    public void testLegacyFunctionsWithDatasets() throws Exception {
        String testDataDSAPACMKTO = "src/test/resources/testdata_legacy_DS_APAC_MKTO.csv";

        String outputFileName = "delta_floating_point.csv";
        CSVPrinter output = null;
        CSVFormat outputCSVFormat = CSVFormat.DEFAULT.withRecordSeparator("\n");

        try (InputStreamReader readerTestDataDSAPACMKTO = new InputStreamReader(
                new BOMInputStream(new FileInputStream(testDataDSAPACMKTO)));
                FileWriter writer = new FileWriter(outputFileName)) {

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

            for (CSVRecord record : CSVFormat.EXCEL.withHeader().parse(readerTestDataDSAPACMKTO)) {

                long startReadTime = System.currentTimeMillis();
                if (lastLoopEndTime > 0) {
                    totalReadTime += startReadTime - lastLoopEndTime;
                }
                i++;

                String id = record.get("LeadID");
                String company = record.get("Company");

                long attributeTime = System.currentTimeMillis();
                totalAttTime = attributeTime - startReadTime;

                Object resultCompanyEntropy = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                        "std_visidb_ds_entropy", "std_visidb_ds_entropy", new String[] { company }, Double.class);

                long fc1Time = System.currentTimeMillis();
                totalFcn1Time += fc1Time - attributeTime;

                List<String> outputRecord = new ArrayList<>();

                Boolean passesCompanyEntropy = Boolean.FALSE;
                if (resultCompanyEntropy == null) {
                    passesCompanyEntropy = (record.get("DS_CompanyName_Entropy").equals(""));
                } else if (record.get("DS_CompanyName_Entropy").equals("")) {
                    passesCompanyEntropy = Boolean.FALSE;
                } else {
                    passesCompanyEntropy = (Math.abs(Double.parseDouble(record.get("DS_CompanyName_Entropy"))
                            - (double) resultCompanyEntropy) < FLOATING_POINT_PRECISION);
                }

                if (DEBUGGING_OUTPUT) {
                    if (WRITE_FLOATING_POINT_DIFFERENCES) {
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

                assertEquals(passesCompanyEntropy, Boolean.TRUE);

                lastLoopEndTime = System.currentTimeMillis();
            }
            if (EXECUTION_TIME_OUTPUT) {
                System.out.println(String.format("Number of rows processed: %d", i));
                System.out.println(String.format("Avg Read Time: %f", (totalReadTime) / ((double) i - 1)));
                System.out.println(String.format("Avg Att Time: %f", ((double) totalAttTime) / ((double) i)));
                System.out.println(String.format("Avg Function (1) Time: %f", ((double) totalFcn1Time) / ((double) i)));
            }
        }
    }
}
