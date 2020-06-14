package com.latticeengines.serviceflows.workflow.stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.ProfileParameters;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.stats.ProfileJobConfig;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

@Component("statsProfiler")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class StatsProfiler {

    private static final Logger log = LoggerFactory.getLogger(StatsProfiler.class);

    @Value("${datacloud.etl.profile.encode.bit:64}")
    private int encodeBits;

    @Value("${datacloud.etl.profile.attrs:1000}")
    private int maxAttrs;

    @Resource(name = "yarnConfiguration")
    protected Configuration yarnConfiguration;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    private AttrClassifier classifier;

    public void initProfileConfig(ProfileJobConfig config) {
        config.setNumericAttrs(new ArrayList<>());
        config.setCatAttrs(new ArrayList<>());
        config.setAmAttrsToEnc(new ArrayList<>());
        config.setExAttrsToEnc(new ArrayList<>());
        config.setCodeBookMap(new HashMap<>());
        config.setCodeBookLookup(new HashMap<>());
    }

    /* Classify an attribute belongs to which scenario: */
    /*- DataCloud ID attr: AccountMasterId */
    /*- Discard attr: attr will not show up in bucketed source */
    /*- No bucket attr: attr will show up in bucketed source and stats, but no bucket created. They are DataCloud attrs which are predefined by PM */
    /*- Pre-known bucket attr: DataCloud attrs whose enum values are pre-known, eg. Intent attributes) */
    /*- Numerical attr */
    /*- Boolean attr */
    /*- Categorical attr */
    /*- Other attr: don't know how to profile it for now, don't create bucket for it */
    /*
     * For AccountMasterStatistics job, we will encode numerical attr and
     * categorical attr, but NO for ProfileAccount job in PA. Because
     * BucketedAccount needs to support decode in Redshift query, but NO for
     * bucketed AccountMaster
     */
    public void classifyAttrs(List<ColumnMetadata> cms, ProfileJobConfig jobConfig) {
        String dataCloudVersion = jobConfig.getDataCloudVersion();
        if (Boolean.TRUE.equals(jobConfig.getConsiderAMAttrs())) {
            dataCloudVersion = columnMetadataProxy.latestVersion().getVersion();
        }
        Map<String, ProfileParameters.Attribute> declaredAttrsConfig = parseDeclaredAttrs(jobConfig);
        Map<String, ProfileArgument> amAttrsConfig;
        if (Boolean.TRUE.equals(jobConfig.getConsiderAMAttrs())) {
//            amAttrsConfig = findAMAttrsConfig(jobConfig, dataCloudVersion);
            amAttrsConfig = new HashMap<>();
        } else {
            amAttrsConfig = new HashMap<>();
        }
        log.info("Classifying attributes...");
        try {
            classifier = new AttrClassifier(jobConfig, amAttrsConfig, declaredAttrsConfig, encodeBits, maxAttrs);
            classifier.classifyAttrs(cms);
        } catch (Exception ex) {
            throw new RuntimeException("Fail to classify attributes", ex);
        }
    }

    public void appendResult(HdfsDataUnit dataUnit) {
        Object[][] data = classifier.parseResult();
        List<Pair<String, Class<?>>> columns = ProfileUtils.getProfileSchema();
        String outputDir = PathUtils.toParquetOrAvroDir(dataUnit.getPath());
        try {
            AvroUtils.createAvroFileByData(yarnConfiguration, columns, data, outputDir, "Profile.avro");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, ProfileParameters.Attribute> parseDeclaredAttrs(ProfileJobConfig jobConfig) {
        Map<String, ProfileParameters.Attribute> profileArgMap = new HashMap<>();
        List<ProfileParameters.Attribute> declaredAttrs = jobConfig.getDeclaredAttrs();
        if (CollectionUtils.isNotEmpty(declaredAttrs)) {
            declaredAttrs.forEach(attr -> {
                String attrName = attr.getAttr();
                profileArgMap.put(attrName, attr);
            });
        }
        return profileArgMap;
    }

}
