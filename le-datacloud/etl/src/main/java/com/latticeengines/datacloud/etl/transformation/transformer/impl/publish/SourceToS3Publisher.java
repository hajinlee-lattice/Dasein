package com.latticeengines.datacloud.etl.transformation.transformer.impl.publish;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.EXPIRE_DAYS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.S3_TO_GLACIER_DAYS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SOURCE_TO_S3_PUBLISHER;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.core.util.PurgeStrategyUtils;
import com.latticeengines.datacloud.core.util.RequestContext;
import com.latticeengines.datacloud.etl.purge.entitymgr.PurgeStrategyEntityMgr;
import com.latticeengines.datacloud.etl.service.SourceHdfsS3TransferService;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.AbstractTransformer;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(SourceToS3Publisher.TRANSFORMER_NAME)
public class SourceToS3Publisher extends AbstractTransformer<TransformerConfig> {
    private static final Logger log = LoggerFactory.getLogger(SourceToS3Publisher.class);

    public static final String TRANSFORMER_NAME = TRANSFORMER_SOURCE_TO_S3_PUBLISHER;

    @Resource(name = "distCpConfiguration")
    private Configuration distCpConfiguration;

    @Inject
    private PurgeStrategyEntityMgr purgeStrategyEntityMgr;

    @Inject
    private S3Service s3Service;

    @Inject
    private SourceHdfsS3TransferService sourceHdfsS3TransferService;

    @Value("${datacloud.collection.s3bucket}")
    private String s3Bucket;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected boolean validateConfig(TransformerConfig config, List<String> sourceNames) {
        if (!(config.validate(sourceNames))) {
            RequestContext.logError("Validation fails due to base source name is empty.");
            return false;
        }
        return true;
    }

    @Override
    protected boolean transformInternal(TransformationProgress progress, String workflowDir, TransformStep step) {
        try {
            for (int i = 0; i < step.getBaseSources().length; i++) {
                Source source = step.getBaseSources()[i];
                String version = step.getBaseVersions().get(i);
                List<Pair<String, String>> tags = getPurgeStrategyTags(source);
                sourceHdfsS3TransferService.transfer(true, source, version, tags, false, false);
                // _CURRENT_VERSION file should not be purged, not to be tagged
                // with purge strategy, remove tags on _CURRENT_VERSION
                String versionPath = hdfsPathBuilder.constructVersionFile(source).toString();
                if (!(source instanceof TableSource)) {
                    s3Service.deleteObjectTags(s3Bucket, versionPath.substring(1));
                }
            }
            step.setTarget(null);
            step.setCount(0L);
            return true;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    private List<Pair<String, String>> getPurgeStrategyTags(Source source) {
        PurgeStrategy ps = purgeStrategyEntityMgr.findStrategyBySourceAndType(source.getSourceName(),
                PurgeStrategyUtils.getSourceType(source));
        Integer glacierDays = (ps == null) || (ps.getGlacierDays() == null) ? 180 : ps.getGlacierDays();
        Integer s3Days = (ps == null) || (ps.getS3Days() == null) ? 180 : ps.getS3Days();
        return Arrays.asList(Pair.of(S3_TO_GLACIER_DAYS, s3Days.toString()),
                Pair.of(EXPIRE_DAYS, Integer.toString(s3Days + glacierDays)));

    }
}
