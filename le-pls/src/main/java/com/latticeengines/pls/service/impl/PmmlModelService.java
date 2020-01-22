package com.latticeengines.pls.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
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

    @Inject
    private Configuration yarnConfiguration;

    protected PmmlModelService() {
        super(ModelType.PMML);
    }

    @Override
    public List<Attribute> getRequiredColumns(String modelId) {
        ModelSummary summary = modelSummaryProxy.findByModelId(MultiTenantContext.getTenant().getId(), modelId,
                true, false, true);
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
        return "";
    }

    @Override
    public Set<String> getLatticeAttributeNames(String modelId) {
        return Collections.emptySet();
    }
}
