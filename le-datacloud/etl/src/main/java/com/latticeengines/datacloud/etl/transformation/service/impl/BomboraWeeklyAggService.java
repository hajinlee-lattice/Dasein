package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.BomboraWeeklyAgg;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BasicTransformationConfiguration;
@Component("bomboraWeeklyAggService")
public class BomboraWeeklyAggService
        extends SimpleTransformationServiceBase<BasicTransformationConfiguration, TransformationFlowParameters>
        implements TransformationService<BasicTransformationConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(BomboraWeeklyAggService.class);

    @Autowired
    private BomboraWeeklyAgg source;

    @Override
    public Source getSource() {
        return source;
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    @Override
    protected String getDataFlowBeanName() {
        return "bomboraWeeklyAggFlow";
    }

    @Override
    protected String getServiceBeanName() {
        return "bomboraWeeklyAggService";
    }

    @Override
    public List<String> findUnprocessedBaseVersions() {
        return Collections.emptyList();
    }

    @Override
    protected List<String> parseBaseVersions(Source baseSource, String baseVersion) {
        String sourceDir = hdfsPathBuilder.constructSnapshotRootDir(baseSource.getSourceName()).toString();
        List<String> baseVersionList = new ArrayList<>();
        try {
            Date baseVersionDate = HdfsPathBuilder.dateFormat.parse(baseVersion);
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            cal.setTime(baseVersionDate);
            cal.add(Calendar.DATE, -6);
            String cufOffVersion = HdfsPathBuilder.dateFormat.format(cal.getTime());

            List<String> versionPaths = HdfsUtils.getFilesForDir(yarnConfiguration, sourceDir);
            Collections.sort(versionPaths, Collections.reverseOrder());
            for (String versionPath : versionPaths) {
                if (HdfsUtils.isDirectory(yarnConfiguration, versionPath)) {
                    versionPath = versionPath.substring(versionPath.indexOf(sourceDir.toString()));
                    String version = new Path(versionPath).getName();
                    Path success = new Path(versionPath, HdfsPathBuilder.SUCCESS_FILE);
                    if (version.compareTo(baseVersion) <= 0 && version.compareTo(cufOffVersion) >= 0
                            && HdfsUtils.fileExists(yarnConfiguration, success.toString())) {
                        baseVersionList.add(version);
                    }
                    if (version.compareTo(cufOffVersion) < 0) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to find avro paths for " + source.getSourceName() + " before " + baseVersion);
        }
        return baseVersionList;
    }
}
