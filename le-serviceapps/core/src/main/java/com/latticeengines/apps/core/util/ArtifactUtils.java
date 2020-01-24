package com.latticeengines.apps.core.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.PivotValuesLookup;
import com.latticeengines.domain.exposed.util.ModelingUtils;
import com.latticeengines.domain.exposed.util.PivotMappingFileUtils;

public final class ArtifactUtils {

    protected ArtifactUtils() {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(ArtifactUtils.class);

    public static Table getPivotedTrainingTable(String pivotArtifactPath, Table trainingTable,
            Configuration configuration) {
        List<Attribute> trainingAttrs = trainingTable.getAttributes();
        PivotValuesLookup pivotValues = null;
        try {
            pivotValues = ModelingUtils.getPivotValues(configuration, pivotArtifactPath);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
        if (pivotValues == null) {
            throw new RuntimeException("PivotValuesLookup is null.");
        }
        Set<String> sourceColumnNames = pivotValues.pivotValuesBySourceColumn.keySet();
        List<Attribute> attrs = PivotMappingFileUtils.createAttrsFromPivotSourceColumns(sourceColumnNames,
                trainingAttrs);

        trainingTable.setAttributes(attrs);
        return trainingTable;

    }

    public static List<Artifact> getArtifacts(String moduleName, ArtifactType artifactType,
            Configuration configuration) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        String path = String.format("%s/%s/%s", //
                PathBuilder.buildMetadataPath(CamilleEnvironment.getPodId(), customerSpace).toString(), //
                moduleName, //
                artifactType.getPathToken());
        List<Artifact> artifacts = new ArrayList<>();
        try {
            List<String> files = HdfsUtils.getFilesForDir(configuration, path);

            for (String filePath : files) {
                Artifact artifact = new Artifact();
                org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(filePath);
                artifact.setName(hadoopPath.getName());
                artifact.setPath(org.apache.hadoop.fs.Path.getPathWithoutSchemeAndAuthority(hadoopPath).toString());
                artifact.setArtifactType(artifactType);
                artifacts.add(artifact);
            }
        } catch (IOException e) {
            log.warn(e.getMessage());
            return new ArrayList<>();
        }
        return artifacts;
    }
}
