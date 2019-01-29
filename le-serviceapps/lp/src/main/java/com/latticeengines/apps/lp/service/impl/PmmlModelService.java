package com.latticeengines.apps.lp.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.lp.util.ModelingHdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;

@Component("pmmlModelService")
public class PmmlModelService extends ModelServiceBase {

    private static final Logger log = LoggerFactory.getLogger(PmmlModelService.class);

    @Value("${pls.modelingservice.basedir}")
    private String modelingBaseDir;

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    protected PmmlModelService() {
        super(ModelType.PMML);
    }

    @Override
    public List<Attribute> getRequiredColumns(String modelId) {
        ModelSummary summary = modelSummaryService.findByModelId(modelId, true, false, true);
        DataComposition datacomposition;
        try {
            datacomposition = getDataComposition(summary);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<Attribute> attributes = new ArrayList<>();

        for (Map.Entry<String, FieldSchema> field : datacomposition.fields.entrySet()) {
            Attribute attr = new Attribute();
            attr.setName(field.getKey());
            attr.setDisplayName(field.getKey());

            attributes.add(attr);
        }
        return attributes;
    }

    private DataComposition getDataComposition(ModelSummary summary) throws IOException {
        final String modelId = UuidUtils.extractUuid(summary.getId());
        String dir = String.format("%s/%s/models", modelingBaseDir, summary.getTenant().getId());

        List<String> modelIdDir = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, dir, new HdfsFileFilter() {

            @Override
            public boolean accept(FileStatus file) {
                if (file.isDirectory() && file.getPath().getName().equals(modelId)) {
                    return true;
                }
                return false;
            }
        }, true);

        List<String> datacomposition = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, modelIdDir.get(0),
                new HdfsFileFilter() {

                    @Override
                    public boolean accept(FileStatus file) {
                        if (file.getPath().getName().equals("datacomposition.json")) {
                            return true;
                        }
                        return false;
                    }
                });
        return JsonUtils.deserialize(HdfsUtils.getHdfsFileContents(yarnConfiguration, datacomposition.get(0)),
                DataComposition.class);
    }

    @Override
    public String copyModel(ModelSummary modelSummary, String sourceTenantId, String targetTenantId) {
        String newModelGuid = null;
        try {
            String eventTableName = modelSummary.getEventTableName();
            if (StringUtils.isEmpty(eventTableName)) {
                eventTableName = ModelingHdfsUtils.getEventTableNameFromHdfs(yarnConfiguration,
                        customerBaseDir + sourceTenantId + "/models", modelSummary.getId(), s3Bucket);
            }
            String cpEventTable = "copy_PMML" + UUID.randomUUID().toString();

            newModelGuid = copyHdfsData(sourceTenantId, targetTenantId, eventTableName, "", cpEventTable, modelSummary);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new LedpException(LedpCode.LEDP_18111,
                    new String[] { modelSummary.getName(), sourceTenantId, targetTenantId });
        }
        return newModelGuid;
    }

    @Override
    public Set<String> getLatticeAttributeNames(String modelId) {
        return Collections.emptySet();
    }

}
