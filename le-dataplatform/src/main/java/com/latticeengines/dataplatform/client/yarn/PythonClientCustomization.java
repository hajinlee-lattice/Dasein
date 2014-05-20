package com.latticeengines.dataplatform.client.yarn;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.yarn.fs.LocalResourcesFactoryBean;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.runtime.python.PythonContainerProperty;
import com.latticeengines.domain.exposed.dataplatform.Classifier;
import com.latticeengines.domain.exposed.dataplatform.DataSchema;
import com.latticeengines.domain.exposed.dataplatform.Field;


public class PythonClientCustomization extends DefaultYarnClientCustomization {

    private static final Log log = LogFactory.getLog(PythonClientCustomization.class);

    public PythonClientCustomization(Configuration configuration) {
        super(configuration);
    }

    @Override
    public String getClientId() {
        return "pythonClient";
    }

    @Override
    public String getContainerLauncherContextFile(Properties properties) {
        return "/python/dataplatform-python-appmaster-context.xml";
    }

    @Override
    public void beforeCreateLocalLauncherContextFile(Properties properties) {
        try {
            String dir = properties.getProperty(ContainerProperty.JOBDIR.name());
            String metadata = properties.getProperty(PythonContainerProperty.METADATA.name());
            Classifier classifier = JsonUtils.deserialize(metadata, Classifier.class);
            properties.put(PythonContainerProperty.TRAINING.name(), classifier.getTrainingDataHdfsPath());
            properties.put(PythonContainerProperty.TEST.name(), classifier.getTestDataHdfsPath());
            properties.put(PythonContainerProperty.PYTHONSCRIPT.name(), classifier.getPythonScriptHdfsPath());
            properties.put(PythonContainerProperty.SCHEMA.name(), classifier.getSchemaHdfsPath());

            File metadataFile = new File(dir  + "/metadata.json");
            FileUtils.writeStringToFile(metadataFile, metadata);
            properties.put(PythonContainerProperty.METADATA_CONTENTS.name(), metadata);
            properties.put(PythonContainerProperty.METADATA.name(), metadataFile.getAbsolutePath());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Collection<CopyEntry> getCopyEntries(Properties containerProperties) {
        Collection<CopyEntry> copyEntries = super.getCopyEntries(containerProperties);
        String metadataFilePath = containerProperties.getProperty(ContainerProperty.METADATA.name());
        copyEntries.add(new LocalResourcesFactoryBean.CopyEntry("file:" + metadataFilePath,
                getJobDir(containerProperties), false));
        return copyEntries;

    }

    @Override
    public void validate(Properties appMasterProperties, Properties containerProperties) {
        String metadata = containerProperties.getProperty(PythonContainerProperty.METADATA.name());
        Classifier classifier = JsonUtils.deserialize(metadata, Classifier.class);
        List<String> features = classifier.getFeatures();
        List<String> targets = classifier.getTargets();
        String schemaHdfsPath = classifier.getSchemaHdfsPath();

        if (features == null || features.size() == 0) {
            throw new LedpException(LedpCode.LEDP_10002);
        }

        if (targets == null || targets.size() != 1) {
            throw new LedpException(LedpCode.LEDP_10003);
        }

        if (schemaHdfsPath == null) {
            throw new LedpException(LedpCode.LEDP_10000);
        }
        String metadataJson = null;
        try {
            metadataJson = HdfsUtils.getHdfsFileContents(configuration, schemaHdfsPath);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_10001, e);
        }

        DataSchema schema = null;
        try {
            schema = JsonUtils.deserialize(metadataJson, DataSchema.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_10005, e);
        }

        List<Field> fields = schema.getFields();
        Set<String> fieldNames = new HashSet<String>();
        for (Field field : fields) {
            fieldNames.add(field.getName());
        }

        for (String feature : features) {
            if (!fieldNames.contains(feature)) {
                throw new LedpException(LedpCode.LEDP_10004, new String[] { feature });
            }
        }
    }

}
