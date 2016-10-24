package com.latticeengines.propdata.engine.transformation.service.impl;

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
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.source.impl.BomboraDepivoted;
import com.latticeengines.propdata.core.source.impl.BomboraWeeklyAgg;
import com.latticeengines.propdata.engine.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

public class BomboraWeeklyAggServiceImplTestNG
        extends TransformationServiceImplTestNGBase<BasicTransformationConfiguration> {
    private String baseSourceVersion = "2016-10-07_00-00-00_UTC";
    private String targetVersion = "2016-10-08_00-00-00_UTC";

    private final static String DOMAIN = "Domain";
    private final static String DATE = "Date";
    private final static String TOPIC = "Topic";
    private final static String ZIP_CODE_OF_HIGHEST_AGGREGATED_SCORE = "ZipCodeOfHighestAggregatedScore";
    private final static String CONTENT_SOURCES = "ContentSources";
    private final static String TOTAL_VIEWS = "TotalViews";
    private final static String UNIQUE_USERS = "UniqueUsers";
    private final static String TOTAL_AGGREGATED_SCORE = "TotalAggregatedScore";
    private final static String HIGHLY_RELEVANT_SOURCES = "HighlyRelevantSources";
    private final static String MOST_RELEVANT_SOURCES = "MostRelevantSources";
    private final static String TOTAL_AGGREGATED_SCORE_HIGHLY_RELEVANT = "TotalAggregatedScore_HighlyRelevant";
    private final static String TOTAL_AGGREGATED_SCORE_MOST_RELEVANT = "TotalAggregatedScore_MostRelevant";

    @Autowired
    BomboraWeeklyAgg source;

    @Autowired
    BomboraDepivoted baseSource;

    @Autowired
    private BomboraWeeklyAggService bomboraWeeklyAggService;

    @Test(groups = "functional")
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
    TransformationService<BasicTransformationConfiguration> getTransformationService() {
        return bomboraWeeklyAggService;
    }

    @Override
    Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(source.getBaseSources()[0], baseSourceVersion).toString();
    }

    @Override
    BasicTransformationConfiguration createTransformationConfiguration() {
        BasicTransformationConfiguration configuration = new BasicTransformationConfiguration();
        configuration.setVersion(targetVersion);
        List<String> baseVersions = new ArrayList<String>();
        baseVersions.add(baseSourceVersion);
        configuration.setBaseVersions(baseVersions);
        return configuration;
    }

    @Override
    protected String getPathForResult() {
        return hdfsPathBuilder.constructSnapshotDir(source, targetVersion).toString();
    }

    protected void uploadBaseAvro(Source baseSource, String baseSourceVersion, boolean isCurrentVersion) {
        try {
            InputStream baseAvroStream = ClassLoader.getSystemResourceAsStream("sources/" + baseSource.getSourceName()
                    + "_" + baseSourceVersion.substring(0, 10).replace("-", "") + ".avro");
            String targetPath = hdfsPathBuilder.constructSnapshotDir(baseSource, baseSourceVersion)
                    .append("part-0000.avro").toString();

            if (HdfsUtils.fileExists(yarnConfiguration, targetPath)) {
                HdfsUtils.rmdir(yarnConfiguration, targetPath);
            }
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, baseAvroStream, targetPath);
            InputStream stream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
            String successPath = hdfsPathBuilder.constructSnapshotDir(baseSource, baseSourceVersion).append("_SUCCESS")
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
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
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
