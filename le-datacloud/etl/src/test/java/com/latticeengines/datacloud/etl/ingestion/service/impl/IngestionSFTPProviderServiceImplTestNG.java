package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.testframework.DataCloudEtlFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.ingestion.SftpConfiguration;
import com.latticeengines.domain.exposed.datacloud.ingestion.VersionCheckStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;

public class IngestionSFTPProviderServiceImplTestNG extends DataCloudEtlFunctionalTestNGBase {
    private static final String POD_ID = IngestionSFTPProviderServiceImplTestNG.class.getSimpleName();

    @Inject
    private IngestionSFTPProviderServiceImpl ingestionSFTPProviderService;

    @BeforeClass(groups = "functional")
    public void setup() {
        prepareCleanPod(POD_ID);
    }

    @AfterClass(groups = "functional")
    public void destroy() {
        prepareCleanPod(POD_ID);
    }

    @Test(groups = "functional")
    public void testGetExistingFiles() {
        // prepare ingestion
        Ingestion ingestion = new Ingestion();
        ingestion.setIngestionName(IngestionSFTPProviderServiceImplTestNG.class.getSimpleName());
        SftpConfiguration config = new SftpConfiguration();
        config.setCheckVersion(1);
        config.setCheckStrategy(VersionCheckStrategy.WEEK);
        config.setFileRegexPattern("AllDomainsAllTopicsZips_(.*).csv.gz");
        ingestion.setProviderConfiguration(config);

        // prepare hdfs files
        String file1 = "AllDomainsAllTopicsZips_20190714_1.csv.gz";
        String file2 = "AllDomainsAllTopicsZips_20190714_2.csv.gz";
        Date current = new Date();
        LocalDateTime localDateTime = current.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        Date lastMonth = Date.from(localDateTime.plusMonths(-1).atZone(ZoneId.systemDefault()).toInstant());
        String oldVersion = HdfsPathBuilder.dateFormat.format(lastMonth);
        uploadBaseSourceFile(new IngestionSource(ingestion.getIngestionName()),
                file1, oldVersion);
        uploadBaseSourceFile(new IngestionSource(ingestion.getIngestionName()),
                file2, oldVersion);
        String currVersion = HdfsPathBuilder.dateFormat.format(current);
        uploadBaseSourceFile(new IngestionSource(ingestion.getIngestionName()),
                file1, currVersion);
        uploadBaseSourceFile(new IngestionSource(ingestion.getIngestionName()),
                file2, currVersion);

        // Based on the check strategy configured in SftpConfiguration, should
        // only return files in current version with _SUCCESS excluded
        List<String> files = ingestionSFTPProviderService.getExistingFiles(ingestion);
        Assert.assertEquals(files.size(), 2);
        Collections.sort(files);
        Assert.assertEquals(files.get(0), file1);
        Assert.assertEquals(files.get(1), file2);

        config.setHasSubfolder(true);
        files = ingestionSFTPProviderService.getExistingFiles(ingestion);
        Assert.assertEquals(files.size(), 2);
        Collections.sort(files);
        Assert.assertEquals(files.get(0), currVersion + "/" + file1);
        Assert.assertEquals(files.get(1), currVersion + "/" + file2);
    }

    @Test(groups = "functional", dataProvider = "filesDiffProvider")
    public void testGetFilesDiff(List<String> targetFiles, List<String> existingFiles, boolean hasSubfolder,
            String subfolderTSPattern, List<String> expectedDiffFiles) {
        Ingestion ingestion = new Ingestion();
        SftpConfiguration config = new SftpConfiguration();
        config.setHasSubfolder(hasSubfolder);
        config.setSubfolderTSPattern(subfolderTSPattern);
        ingestion.setProviderConfiguration(config);

        List<String> diffFiles = ingestionSFTPProviderService.getFilesDiff(ingestion, targetFiles, existingFiles);
        Collections.sort(diffFiles);
        Collections.sort(expectedDiffFiles);
        Assert.assertEquals(diffFiles, expectedDiffFiles);
    }

    // Schema: targetFiles, existingFiles, hasSubfolder, subfolderTSPattern,
    // expectedDiffFiles
    @DataProvider(name = "filesDiffProvider")
    private Object[][] getFilesDiffProvider() {
        return new Object[][] { //
                // CASE 1: If there is no subfolder, both target files and
                // existing files only contain file names, then compare diff
                // directly
                { Arrays.asList("AAA", "BBB"), Arrays.asList("BBB", "AAA"), false, null, Collections.emptyList() }, //
                { Arrays.asList("AAA", "BBB"), Arrays.asList("AAA"), false, null, Arrays.asList("BBB") }, //
                { Arrays.asList("AAA", "BBB"), Arrays.asList("BBB", "CCC"), false, null, Arrays.asList("AAA") }, //

                // CASE 2: If subfolder is not timestamp versioned, extract file
                // names from target files and existing files (subfolder is
                // ignored), then compare file name diff
                { Arrays.asList("aaa/AAA", "bbb/BBB"), Arrays.asList("bbb/BBB", "aaa/AAA"), true, null,
                        Collections.emptyList() }, //
                { Arrays.asList("aaa/AAA", "bbb/BBB"), Arrays.asList("aaa/AAA"), true, null,
                        Arrays.asList("bbb/BBB") }, //
                { Arrays.asList("aaa/AAA", "bbb/BBB"), Arrays.asList("BBB", "CCC"), true, null,
                        Arrays.asList("aaa/AAA") }, //

                // CASE 3: If subfolder is timestamp versioned, extract version
                // from both target files and existing files, then compare file
                // name diff for each version
                { Arrays.asList("20190101/AAA"), Arrays.asList("2019-01-01_00-00-00_UTC/AAA"), true, "yyyyMMdd",
                        Collections.emptyList() }, //
                { Arrays.asList("20190101/AAA", "20190101/BBB"), Arrays.asList("2019-01-01_00-00-00_UTC/AAA"), true,
                        "yyyyMMdd", Arrays.asList("20190101/BBB") }, //
                { Arrays.asList("20190101/AAA", "20190101/BBB"), Collections.emptyList(), true, "yyyyMMdd",
                        Arrays.asList("20190101/AAA", "20190101/BBB") }, //
                { Arrays.asList("20190101/AAA", "20190101/BBB"),
                        Arrays.asList("2019-01-01_00-00-00_UTC/AAA", "2019-01-01_00-00-00_UTC/CCC"), true, "yyyyMMdd",
                        Arrays.asList("20190101/BBB") }, //

                { Arrays.asList("20190101/AAA", "20190102/BBB"), Arrays.asList("2019-01-01_00-00-00_UTC/AAA"), true,
                        "yyyyMMdd", Arrays.asList("20190102/BBB") }, //
                { Arrays.asList("20190101/AAA", "20190102/AAA"), Arrays.asList("2019-01-01_00-00-00_UTC/AAA"), true,
                        "yyyyMMdd", Arrays.asList("20190102/AAA") }, //
                { Arrays.asList("20190101/AAA", "20190101/BBB", "20190102/AAA"),
                        Arrays.asList("2019-01-01_00-00-00_UTC/AAA"), true, "yyyyMMdd",
                        Arrays.asList("20190101/BBB", "20190102/AAA") }, //
        };
    }

}
