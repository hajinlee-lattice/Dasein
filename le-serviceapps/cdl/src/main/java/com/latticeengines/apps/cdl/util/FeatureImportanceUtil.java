package com.latticeengines.apps.cdl.util;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;

import reactor.core.publisher.Flux;

@Component
public class FeatureImportanceUtil {

    private static Logger log = LoggerFactory.getLogger(FeatureImportanceUtil.class);

    @Value("${cdl.feature.importance.transform.suffixes}")
    private String featureImportanceTransformSuffixes;

    @Value("${cdl.feature.importance.special.suffix}")
    private String featureImportanceSpecialSuffix;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Value("${camille.zk.pod.id:Default}")
    private String podId;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Inject
    private Configuration yarnConfiguration;

    private List<String> getFeatureImportanceTransformSuffixes() {
        return Arrays.asList(featureImportanceTransformSuffixes.split(","));
    }

    public Map<String, Integer> getFeatureImportance(String customerSpace, ModelSummary modelSummary) {
        try {
            String featureImportanceFilePathPattern = "{0}/{1}/models/{2}/{3}/{4}/rf_model.txt";

            String[] filePathParts = modelSummary.getLookupId().split("\\|");
            String featureImportanceFilePath = MessageFormat.format(featureImportanceFilePathPattern, //
                    modelingServiceHdfsBaseDir, // 0
                    filePathParts[0], // 1
                    filePathParts[1], // 2
                    filePathParts[2], // 3
                    ApplicationIdUtils.stripJobId(modelSummary.getApplicationId())); // 4
            featureImportanceFilePath = getS3Path(customerSpace, featureImportanceFilePath);
            if (!HdfsUtils.fileExists(yarnConfiguration, featureImportanceFilePath)) {
                log.error("Failed to find the feature importance file: " + featureImportanceFilePath);
                throw new LedpException(LedpCode.LEDP_10011, new String[] { featureImportanceFilePath });
            }
            log.info("Attempting to get feature importance from the file: " + featureImportanceFilePath);
            String featureImportanceRaw = HdfsUtils.getHdfsFileContents(yarnConfiguration, featureImportanceFilePath);
            if (StringUtils.isEmpty(featureImportanceRaw)) {
                log.error("Failed to find the feature importance file: " + featureImportanceFilePath);
                throw new LedpException(LedpCode.LEDP_40037,
                        new String[] { featureImportanceFilePath, modelSummary.getId(), customerSpace });
            }
            TreeMap<Double, String> sortedImportance = Flux.fromArray(featureImportanceRaw.split("\n"))
                    .collect(TreeMap<Double, String>::new, (sortedMap, line) -> {
                        try {
                            sortedMap.put(Double.parseDouble(line.split(",")[1]), line.split(",")[0]);
                        } catch (NumberFormatException e) {
                            // ignore since this is for the first row
                        }
                    }).block();

            AtomicInteger i = new AtomicInteger(1);
            Map<String, Integer> importanceOrdering = Flux.fromIterable(sortedImportance.descendingMap().entrySet())
                    .collect(HashMap<String, Integer>::new, (map, es) -> map.put(es.getValue(), i.getAndIncrement()))
                    .block();

            return convertDerivedAttrNamesToParentAttrNames(importanceOrdering);
        } catch (Exception e) {
            log.error("Unable to populate feature importance due to " + e.getLocalizedMessage());
            return new HashMap<>();
        }
    }

    private String getS3Path(String customerSpace, String featureImportanceFilePath) throws IOException {
        String protocol = Boolean.TRUE.equals(useEmr) ? "s3a" : "s3n";
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder(protocol);
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        String s3Path = pathBuilder.exploreS3FilePath(featureImportanceFilePath, s3Bucket);
        if (HdfsUtils.fileExists(yarnConfiguration, s3Path)) {
            featureImportanceFilePath = s3Path;
        }
        return featureImportanceFilePath;
    }

    private Map<String, Integer> convertDerivedAttrNamesToParentAttrNames(Map<String, Integer> importanceOrdering) {
        Set<String> attributeNames = importanceOrdering.keySet();
        Map<String, Integer> toReturn = new HashMap<>();

        attributeNames.forEach(attrName -> {
            if (attrName.contains(featureImportanceSpecialSuffix)) {
                // find all with range replace them all with parent with the
                // least importance
                String parentAttributeName = attrName.substring(0, attrName.indexOf(featureImportanceSpecialSuffix));
                String mostImportantDerivedAttr = attributeNames.stream()
                        .filter(attr -> attr.startsWith(parentAttributeName)) //
                        .reduce(parentAttributeName,
                                (attr1, attr2) -> importanceOrdering.getOrDefault(attr1,
                                        Integer.MAX_VALUE) <= importanceOrdering.getOrDefault(attr2, Integer.MAX_VALUE)
                                                ? attr1 : attr2);
                toReturn.put(parentAttributeName, importanceOrdering.get(mostImportantDerivedAttr));
            } else if (getFeatureImportanceTransformSuffixes().stream().anyMatch(
                    suffix -> attrName.endsWith(suffix) && !attrName.contains(featureImportanceSpecialSuffix))) {
                String matchedSuffix = getFeatureImportanceTransformSuffixes().stream().filter(attrName::contains)
                        .findFirst().get();
                String parentAttributeName = attrName.replace(matchedSuffix, "");
                toReturn.put(parentAttributeName,
                        Math.min(importanceOrdering.getOrDefault(parentAttributeName, Integer.MAX_VALUE),
                                importanceOrdering.get(attrName)));
            } else {
                toReturn.put(attrName, importanceOrdering.get(attrName));
            }

        });
        return toReturn;
    }
}
