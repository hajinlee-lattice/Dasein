package com.latticeengines.spark.exposed.job.cdl;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.GenerateRecommendationCSVContext;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmakercore.RecommendationColumnName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.GenerateRecommendationCSVConfig;
import com.latticeengines.domain.exposed.util.ExportUtils;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class GenerateRecommendationCSVJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(GenerateRecommendationCSVJobTestNG.class);

    private Object[][] recommendations;
    private Object[][] accounts;
    private static final String destinationAccountId = "D41000001Q3z4EAC";
    private static final String ratingId = RatingEngine.generateIdStr();
    private static final int addOrDeleteContactPerAccount = 5;
    private List<Pair<String, Class<?>>> recommendationFields;
    private List<String> fields;

    @Test(groups = "functional")
    public void testGenerateRecommendationCSV() {
        uploadInputAvro();
        GenerateRecommendationCSVConfig config = buildGenerateRecommendationCSVConfig();
        SparkJobResult result = runSparkJob(GenerateRecommendationCSVJob.class, config);
        verifyResult(result);
    }

    private GenerateRecommendationCSVConfig buildGenerateRecommendationCSVConfig() {
        GenerateRecommendationCSVConfig generateRecommendationCSVConfig = new GenerateRecommendationCSVConfig();
        GenerateRecommendationCSVContext generateRecommendationCSVContext = new GenerateRecommendationCSVContext();
        generateRecommendationCSVContext.setIgnoreAccountsWithoutContacts(true);
        fields = recommendationFields.stream().map(pair -> pair.getLeft()).collect(Collectors.toList());
        fields.add(2, InterfaceName.DUNS.name());
        fields.add(27, InterfaceName.DoNotMail.name());
        generateRecommendationCSVContext.setFields(fields);
        Map<String, String> displayNames = fields.stream()
                .collect(Collectors.toMap(str -> str, str -> {
                    if (InterfaceName.CompanyName.name().equals(str)) {
                        return "Customer" + InterfaceName.CompanyName.name();
                    } else {
                        return str;
                    }
                }));
        generateRecommendationCSVContext.setDisplayNames(displayNames);
        generateRecommendationCSVConfig.setGenerateRecommendationCSVContext(generateRecommendationCSVContext);
        generateRecommendationCSVConfig.setTargetNums(1);
        return generateRecommendationCSVConfig;
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        String outputDir = tgt.getPath();
        String csvPath;
        try {
            List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, outputDir, (HdfsUtils.HdfsFilenameFilter) filename -> filename.endsWith(".csv"));
            csvPath = files.get(0);
            InputStream inputStream = HdfsUtils.getInputStream(yarnConfiguration, csvPath);
            Reader in = new InputStreamReader(inputStream);
            CSVParser records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
            Map<String, Integer> headerMap = records.getHeaderMap();
            log.info("Recommendation header map is {}.", headerMap);
            Assert.assertEquals(headerMap.size(), fields.size());
            Assert.assertEquals(records.getRecords().size(), 50L);
            Assert.assertTrue(headerMap.containsKey(InterfaceName.DUNS.name()));
            Assert.assertTrue(headerMap.containsKey(InterfaceName.DoNotMail.name()));
            Assert.assertTrue(headerMap.get(InterfaceName.DoNotMail.name()) < headerMap.get(ExportUtils.CONTACT_ATTR_PREFIX + InterfaceName.CreatedDate.name()));
            Assert.assertTrue(headerMap.get(InterfaceName.DUNS.name()) < headerMap.get(InterfaceName.DoNotMail.name()));
            Assert.assertTrue(headerMap.containsKey("Customer" + InterfaceName.CompanyName.name()));
        } catch (IOException e) {
            throw new RuntimeException("Failed to read " + outputDir);
        }
        return true;
    }

    private void uploadInputAvro() {
        String playId = "play_" + UUID.randomUUID().toString();
        String launchId = "launch_" + UUID.randomUUID().toString();
        String syncDestination = CDLExternalSystemType.FILE_SYSTEM.name();
        recommendationFields = Arrays.asList( //
                Pair.of(InterfaceName.AccountId.name(), String.class), //
                Pair.of(InterfaceName.CustomerAccountId.name(), String.class), //
                Pair.of(destinationAccountId, String.class), //
                Pair.of(InterfaceName.CompanyName.name(), String.class), //
                Pair.of(InterfaceName.LDC_Name.name(), String.class), //
                Pair.of(ratingId + PlaymakerConstants.RatingScoreColumnSuffix, Integer.class), //
                Pair.of(ratingId, String.class), //
                Pair.of(ratingId + PlaymakerConstants.RatingEVColumnSuffix, String.class), //
                Pair.of(InterfaceName.Website.name(), String.class), //
                Pair.of(InterfaceName.CreatedDate.name(), String.class), //
                Pair.of(RecommendationColumnName.PLAY_ID.name(), String.class), //
                Pair.of(RecommendationColumnName.LAUNCH_ID.name(), String.class), //
                Pair.of(RecommendationColumnName.SYNC_DESTINATION.name(), String.class), //
                Pair.of(ExportUtils.CONTACT_ATTR_PREFIX + InterfaceName.ContactId.name(), String.class), //
                Pair.of(ExportUtils.CONTACT_ATTR_PREFIX + InterfaceName.CustomerContactId.name(), String.class), //
                Pair.of(ExportUtils.CONTACT_ATTR_PREFIX + InterfaceName.CompanyName.name(), String.class), //
                Pair.of(ExportUtils.CONTACT_ATTR_PREFIX + InterfaceName.Email.name(), String.class), //
                Pair.of(ExportUtils.CONTACT_ATTR_PREFIX + InterfaceName.ContactName.name(), String.class), //
                Pair.of(ExportUtils.CONTACT_ATTR_PREFIX + InterfaceName.City.name(), String.class), //
                Pair.of(ExportUtils.CONTACT_ATTR_PREFIX + InterfaceName.State.name(), String.class), //
                Pair.of(ExportUtils.CONTACT_ATTR_PREFIX + InterfaceName.Country.name(), String.class), //
                Pair.of(ExportUtils.CONTACT_ATTR_PREFIX + InterfaceName.PostalCode.name(), String.class), //
                Pair.of(ExportUtils.CONTACT_ATTR_PREFIX + InterfaceName.PhoneNumber.name(), String.class), //
                Pair.of(ExportUtils.CONTACT_ATTR_PREFIX + InterfaceName.Title.name(), String.class), //
                Pair.of(ExportUtils.CONTACT_ATTR_PREFIX + InterfaceName.FirstName.name(), String.class), //
                Pair.of(ExportUtils.CONTACT_ATTR_PREFIX + InterfaceName.LastName.name(), String.class), //
                Pair.of(ExportUtils.CONTACT_ATTR_PREFIX + InterfaceName.CreatedDate.name(), String.class) //
        );
        accounts = new Object[][]{{"0L", "0000", "destinationAccountId", "Lattice", "Lattice Engines", 98, "A", "1000",
                "www.lattice-engines.com", "01/01/2019"}, //
                {"1L", "0001", "destinationAccountId", "DnB", "DnB", 97, "B", "2000", "www.dnb.com", "01/01/2019"}, //
                {"2L", "0002", "destinationAccountId", "Google", "Google", 98, "C", "3000", "www.google.com", "01/01/2019"}, //
                {"3L", "0003", "destinationAccountId", "Facebook", "FB", 93, "E", "1000000", "www.facebook.com", "01/01/2019"}, //
                {"4L", "0004", "destinationAccountId", "Apple", "Apple", null, null, null, "www.apple.com", "01/01/2019"}, //
                {"5L", "0005", "destinationAccountId", "SalesForce", "SalesForce", null, "A", null, "www.salesforce.com", "01/01/2019"}, //
                {"6L", "0006", "destinationAccountId", "Adobe", "Adobe", 98, null, "1000", "www.adobe.com", "01/01/2019"}, //
                {"7L", "0007", "destinationAccountId", "Eloqua", "Eloqua", 40, "F", "100", "www.eloqua.com", "01/01/2019"}, //
                {"8L", "0008", "destinationAccountId", "Dell", "Dell", 8, "F", "10", "www.dell.com", "01/01/2019"}, //
                {"9L", "0009", "destinationAccountId", "HP", "HP", 38, "E", "500", "www.hp.com", "01/01/2019"}, //
                {"100L", "0100", "destinationAccountId", "Fake Co", "Fake Co", 3, "F", "5", "", ""}};
        recommendations = new Object[51][recommendationFields.size()];
        for (int i = 0; i < accounts.length - 1; i++) {
            for (int j = 0; j < addOrDeleteContactPerAccount; j++) {
                recommendations[addOrDeleteContactPerAccount * i + j][0] = accounts[i][0];
                recommendations[addOrDeleteContactPerAccount * i + j][1] = accounts[i][1];
                recommendations[addOrDeleteContactPerAccount * i + j][2] = accounts[i][2];
                recommendations[addOrDeleteContactPerAccount * i + j][3] = accounts[i][3];
                recommendations[addOrDeleteContactPerAccount * i + j][4] = accounts[i][4];
                recommendations[addOrDeleteContactPerAccount * i + j][5] = accounts[i][5];
                recommendations[addOrDeleteContactPerAccount * i + j][6] = accounts[i][6];
                recommendations[addOrDeleteContactPerAccount * i + j][7] = accounts[i][7];
                recommendations[addOrDeleteContactPerAccount * i + j][8] = accounts[i][8];
                recommendations[addOrDeleteContactPerAccount * i + j][9] = accounts[i][9];
                recommendations[addOrDeleteContactPerAccount * i + j][10] = playId;
                recommendations[addOrDeleteContactPerAccount * i + j][11] = launchId;
                recommendations[addOrDeleteContactPerAccount * i + j][12] = syncDestination;
                recommendations[addOrDeleteContactPerAccount * i + j][13] = String.valueOf(addOrDeleteContactPerAccount * i + j);
                recommendations[addOrDeleteContactPerAccount * i + j][14] = accounts.length * i + j + "L";
                recommendations[addOrDeleteContactPerAccount * i + j][15] = "Kind Inc.";
                recommendations[addOrDeleteContactPerAccount * i + j][16] = "michael@kind.com";
                recommendations[addOrDeleteContactPerAccount * i + j][17] = "Michael Jackson";
                recommendations[addOrDeleteContactPerAccount * i + j][18] = "SMO";
                recommendations[addOrDeleteContactPerAccount * i + j][19] = "CA";
                recommendations[addOrDeleteContactPerAccount * i + j][20] = "US";
                recommendations[addOrDeleteContactPerAccount * i + j][21] = "94404";
                recommendations[addOrDeleteContactPerAccount * i + j][22] = "650-898-3928";
                recommendations[addOrDeleteContactPerAccount * i + j][23] = "CEO";
                recommendations[addOrDeleteContactPerAccount * i + j][24] = "Michael";
                recommendations[addOrDeleteContactPerAccount * i + j][25] = "Jackson";
                recommendations[addOrDeleteContactPerAccount * i + j][26] = "08/08/2019";
            }
        }
        recommendations[50][0] = accounts[10][0];
        recommendations[50][1] = accounts[10][1];
        recommendations[50][2] = accounts[10][2];
        recommendations[50][3] = accounts[10][3];
        recommendations[50][4] = accounts[10][4];
        recommendations[50][5] = accounts[10][5];
        recommendations[50][6] = accounts[10][6];
        recommendations[50][7] = accounts[10][7];
        recommendations[50][8] = accounts[10][8];
        recommendations[50][9] = accounts[10][9];
        recommendations[50][10] = playId;
        recommendations[50][11] = launchId;
        recommendations[50][12] = syncDestination;
        uploadHdfsDataUnit(recommendations, recommendationFields);
    }
}
