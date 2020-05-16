package com.latticeengines.datacloud.etl.transformation.transformer.impl.stats;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.stats.CalcStatsTxfmr.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_CALC_STATS_TXMFR;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.dataflow.utils.BucketEncodeUtils;
import com.latticeengines.datacloud.etl.transformation.TransformerUtils;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.ConfigurableSparkJobTxfmr;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.spark.stats.CalcStatsConfig;
import com.latticeengines.spark.exposed.job.stats.CalcStatsJob;

@Component(TRANSFORMER_NAME)
public class CalcStatsTxfmr extends ConfigurableSparkJobTxfmr<CalcStatsConfig> {

    private static final Logger log = LoggerFactory.getLogger(CalcStatsTxfmr.class);

    public static final String TRANSFORMER_NAME = TRANSFORMER_CALC_STATS_TXMFR;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<CalcStatsJob> getSparkJobClz() {
        return CalcStatsJob.class;
    }

    @Override
    protected Class<CalcStatsConfig> getJobConfigClz() {
        return CalcStatsConfig.class;
    }

    @Override
    protected void preSparkJobProcessing(TransformStep step, String workflowDir, CalcStatsConfig jobConfig) {
        Source profileSource = step.getBaseSources()[1];
        String profileVersion = step.getBaseVersions().get(1);

        if (!isProfileSource(profileSource, profileVersion)) {
            profileSource = step.getBaseSources()[0];
            profileVersion = step.getBaseVersions().get(0);
            if (!isProfileSource(profileSource, profileVersion)) {
                throw new RuntimeException("Neither base source has the profile schema");
            } else {
                log.info("Resolved the first base source as profile.");
                List<DataUnit> inputs = new ArrayList<>();
                inputs.add(jobConfig.getInput().get(1));
                inputs.add(jobConfig.getInput().get(0));
                jobConfig.setInput(inputs);
            }
        } else {
            log.info("Resolved the second base source as profile.");
        }
    }

    private boolean isProfileSource(Source source, String version) {
        String avroPath = TransformerUtils.avroPath(source, version, hdfsPathBuilder);
        Iterator<GenericRecord> records = AvroUtils.iterateAvroFiles(yarnConfiguration, avroPath);
        if (records.hasNext()) {
            GenericRecord record = records.next();
            return BucketEncodeUtils.isProfile(record);
        }
        return false;
    }

}
