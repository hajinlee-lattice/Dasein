package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.BomboraDepivoted;
import com.latticeengines.datacloud.core.source.impl.BomboraWeeklyAgg;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BasicTransformationConfiguration;

public class BomboraWeeklyAggServiceImplTestNG
        extends TransformationServiceImplTestNGBase<BasicTransformationConfiguration> {
    private String baseSourceVersion = "2016-10-07_00-00-00_UTC";
    private String targetVersion = "2016-10-08_00-00-00_UTC";

    private static final String DOMAIN = "Domain";
    private static final String DATE = "Date";
    private static final String TOPIC = "Topic";
    private static final String ZIP_CODE_OF_HIGHEST_AGGREGATED_SCORE = "ZipCodeOfHighestAggregatedScore";
    private static final String CONTENT_SOURCES = "ContentSources";
    private static final String TOTAL_VIEWS = "TotalViews";
    private static final String UNIQUE_USERS = "UniqueUsers";
    private static final String TOTAL_AGGREGATED_SCORE = "TotalAggregatedScore";
    private static final String HIGHLY_RELEVANT_SOURCES = "HighlyRelevantSources";
    private static final String MOST_RELEVANT_SOURCES = "MostRelevantSources";
    private static final String TOTAL_AGGREGATED_SCORE_HIGHLY_RELEVANT = "TotalAggregatedScore_HighlyRelevant";
    private static final String TOTAL_AGGREGATED_SCORE_MOST_RELEVANT = "TotalAggregatedScore_MostRelevant";

    @Autowired
    BomboraWeeklyAgg source;

    @Autowired
    BomboraDepivoted baseSource;

    @Autowired
    private BomboraWeeklyAggService bomboraWeeklyAggService;

    @Test(groups = "pipeline2")
    public void testTransformation() {
        uploadBaseAvro(baseSource, "2016-10-08_00-00-00_UTC", true);
        uploadBaseAvro(baseSource, "2016-10-07_00-00-00_UTC", false);
        uploadBaseAvro(baseSource, "2016-10-06_00-00-00_UTC", false);
        uploadBaseAvro(baseSource, "2016-10-05_00-00-00_UTC", false);
        uploadBaseAvro(baseSource, "2016-10-04_00-00-00_UTC", false);
        uploadBaseAvro(baseSource, "2016-10-01_00-00-00_UTC", false);
        uploadBaseAvro(baseSource, "2016-09-30_00-00-00_UTC", false);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected TransformationService<BasicTransformationConfiguration> getTransformationService() {
        return bomboraWeeklyAggService;
    }

    @Override
    protected Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(source.getBaseSources()[0].getSourceName(), baseSourceVersion)
                .toString();
    }

    @Override
    protected BasicTransformationConfiguration createTransformationConfiguration() {
        BasicTransformationConfiguration configuration = new BasicTransformationConfiguration();
        configuration.setVersion(targetVersion);
        List<String> baseVersions = new ArrayList<String>();
        baseVersions.add(baseSourceVersion);
        configuration.setBaseVersions(baseVersions);
        return configuration;
    }

    @Override
    protected String getPathForResult() {
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    protected void uploadBaseAvro(Source baseSource, String baseSourceVersion, boolean isCurrentVersion) {
        try {
            InputStream baseAvroStream = ClassLoader.getSystemResourceAsStream("sources/" + baseSource.getSourceName()
                    + "_" + baseSourceVersion.substring(0, 10).replace("-", "") + ".avro");
            String targetPath = hdfsPathBuilder.constructSnapshotDir(baseSource.getSourceName(), baseSourceVersion)
                    .append("part-0000.avro").toString();

            if (HdfsUtils.fileExists(yarnConfiguration, targetPath)) {
                HdfsUtils.rmdir(yarnConfiguration, targetPath);
            }
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, baseAvroStream, targetPath);
            InputStream stream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
            String successPath = hdfsPathBuilder.constructSnapshotDir(baseSource.getSourceName(), baseSourceVersion)
                    .append("_SUCCESS")
                    .toString();
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, successPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (isCurrentVersion) {
            hdfsSourceEntityMgr.setCurrentVersion(baseSource, baseSourceVersion);
        }
    }

    @SuppressWarnings("static-access")
    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        try {
            Date aggDate = hdfsPathBuilder.dateFormat.parse(targetVersion);
            int rowNum = 0;
            while (records.hasNext()) {
                GenericRecord record = records.next();
                String domain = String.valueOf(record.get(DOMAIN));
                Long date = (Long) record.get(DATE);
                String topic = String.valueOf(record.get(TOPIC));
                String zipCodeOfHighestAggregatedScore = String
                        .valueOf(record.get(ZIP_CODE_OF_HIGHEST_AGGREGATED_SCORE));
                Integer contentSources = (Integer) record.get(CONTENT_SOURCES);
                Integer highlyRelevantSources = (Integer) record.get(HIGHLY_RELEVANT_SOURCES);
                Integer mostRelevantSources = (Integer) record.get(MOST_RELEVANT_SOURCES);
                Integer totalViews = (Integer) record.get(TOTAL_VIEWS);
                Integer uniqueUsers = (Integer) record.get(UNIQUE_USERS);
                Float totalAggregatedScore = (Float) record.get(TOTAL_AGGREGATED_SCORE);
                Float totalAggregatedScoreHighlyRelevant = (Float) record.get(TOTAL_AGGREGATED_SCORE_HIGHLY_RELEVANT);
                Float totalAggregatedScoreMostRelevant = (Float) record.get(TOTAL_AGGREGATED_SCORE_MOST_RELEVANT);
                Assert.assertTrue((domain.equals("google.com") && date.equals(aggDate.getTime())
                        && topic.equals("Big Data") && zipCodeOfHighestAggregatedScore.equals("94043")
                        && contentSources.equals(89) && totalViews.equals(996) && uniqueUsers.equals(794)
                        && Math.abs(totalAggregatedScore - 248.3) < 0.01 && highlyRelevantSources == null
                        && mostRelevantSources == null && totalAggregatedScoreHighlyRelevant == null
                        && totalAggregatedScoreMostRelevant == null)
                        || (domain.equals("uber.com") && date.equals(aggDate.getTime()) && topic.equals("Big Data")
                                && zipCodeOfHighestAggregatedScore.equals("94133") && contentSources.equals(5)
                                && totalViews.equals(15) && uniqueUsers.equals(8)
                                && Math.abs(totalAggregatedScore - 3.2) < 0.01
                                && highlyRelevantSources == null && mostRelevantSources == null
                                && totalAggregatedScoreHighlyRelevant == null
                                && totalAggregatedScoreMostRelevant == null)
                        || (domain.equals("uber.com") && date.equals(aggDate.getTime()) && topic.equals("Apple (AAPL)")
                                && zipCodeOfHighestAggregatedScore.equals("94103") && contentSources.equals(39)
                                && totalViews.equals(449) && uniqueUsers.equals(135)
                                && Math.abs(totalAggregatedScore - 92.71) < 0.01 && highlyRelevantSources == null
                                && mostRelevantSources == null && totalAggregatedScoreHighlyRelevant == null
                                && totalAggregatedScoreMostRelevant == null)
                        || (domain.equals("google.com") && date.equals(aggDate.getTime())
                                && topic.equals("Apple (AAPL)") && zipCodeOfHighestAggregatedScore.equals("94043")
                                && contentSources.equals(696) && totalViews.equals(67162) && uniqueUsers.equals(41103)
                                && Math.abs(totalAggregatedScore - 10305.9) < 0.01 && highlyRelevantSources == null
                                && mostRelevantSources == null && totalAggregatedScoreHighlyRelevant == null
                                && totalAggregatedScoreMostRelevant == null));
                rowNum++;
            }
            Assert.assertEquals(rowNum, 4);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
