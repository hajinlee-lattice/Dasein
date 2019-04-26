package com.latticeengines.cdl.workflow.steps.export;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.cdl.ExportEntity;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.EntityExportStepConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.BaseSparkStep;


@Component("extractAtlasEntity")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExtractAtlasEntity extends BaseSparkStep<EntityExportStepConfiguration> {

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        String workspace = PathBuilder.buildRandomWorkspacePath(podId, customerSpace).toString();
        putObjectInContext(ATLAS_EXPORT_DATA_UNIT, fakeOutputUntis(workspace));
    }

    private Map<ExportEntity, HdfsDataUnit> fakeOutputUntis(String workspace) {
        HdfsDataUnit account = extractFakeAvro(workspace, "Account.avro", 0);
        HdfsDataUnit contact = extractFakeAvro(workspace, "Contact.avro", 1);
        return ImmutableMap.of(ExportEntity.Account, account, ExportEntity.Contact, contact);
    }

    private HdfsDataUnit extractFakeAvro(String workspace, String avroName, int outputIdx) {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream( //
                "com/latticeengines/cdl/workflow/" + avroName);
        String hdfsPath = workspace + "/Output" + outputIdx + "/part-0000.avro";
        try {
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, is, hdfsPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to copy avro to " + hdfsPath);
        }
        HdfsDataUnit dataUnit = new HdfsDataUnit();
        dataUnit.setPath(workspace + "/Output" + outputIdx);
        dataUnit.setCount(900L);
        return dataUnit;
    }

}
