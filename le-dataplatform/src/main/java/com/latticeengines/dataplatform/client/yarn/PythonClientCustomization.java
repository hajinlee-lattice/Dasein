package com.latticeengines.dataplatform.client.yarn;

import static com.latticeengines.dataplatform.runtime.mapreduce.python.PythonMRUtils.getScriptPathWithVersion;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.yarn.fs.LocalResourcesFactoryBean;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.Field;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.hadoop.exposed.service.ManifestService;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;
import com.latticeengines.yarn.exposed.client.ContainerProperty;
import com.latticeengines.yarn.exposed.client.DefaultYarnClientCustomization;
import com.latticeengines.yarn.exposed.runtime.python.PythonContainerProperty;
import com.latticeengines.yarn.exposed.service.EMREnvService;

@Component("pythonClientCustomization")
public class PythonClientCustomization extends DefaultYarnClientCustomization {

    private static final Logger log = LoggerFactory.getLogger(PythonClientCustomization.class);

    @Value("${dataplatform.hdfs.stack}")
    private String stackName;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Inject
    private EMREnvService emrEnvService;

    @Inject
    private EMRCacheService emrCacheService;

    @Inject
    private ManifestService manifestService;

    public PythonClientCustomization() {
        super(null, null, null, null, null, null);
    }

    @Inject
    public PythonClientCustomization(Configuration yarnConfiguration, //
            VersionManager versionManager, //
            @Value("${dataplatform.hdfs.stack:}") String stackName, //
            SoftwareLibraryService softwareLibraryService, //
            @Value("${dataplatform.yarn.job.basedir}") String hdfsJobBaseDir, //
            @Value("${hadoop.fs.web.defaultFS}") String webHdfs) {
        super(yarnConfiguration, versionManager, stackName, softwareLibraryService, hdfsJobBaseDir,
                webHdfs);
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
            log.info("Classifier in PythonContainerProperty is " + JsonUtils.pprint(classifier));
            properties.put(PythonContainerProperty.TRAINING.name(), classifier.getTrainingDataHdfsPath());
            properties.put(PythonContainerProperty.TEST.name(), classifier.getTestDataHdfsPath());
            properties.put(PythonContainerProperty.SCHEMA.name(), classifier.getSchemaHdfsPath());
            properties.put(PythonContainerProperty.DATAPROFILE.name(), classifier.getDataProfileHdfsPath());
            properties.put(PythonContainerProperty.CONFIGMETADATA.name(), classifier.getConfigMetadataHdfsPath());

            String version = manifestService.getLedsVersion();
            properties.put(PythonContainerProperty.VERSION.name(), version);
            String pipelineDriverPath = getScriptPathWithVersion(classifier.getPipelineDriver(), version);
            properties.put(PythonContainerProperty.PIPELINEDRIVER.name(), pipelineDriverPath);
            String pythonScriptPath = getScriptPathWithVersion(classifier.getPythonScriptHdfsPath(), version);
            properties.put(PythonContainerProperty.PYTHONSCRIPT.name(), pythonScriptPath);
            String pipelineScript = getScriptPathWithVersion(classifier.getPythonPipelineScriptHdfsPath(), version);
            properties.put(PythonContainerProperty.PYTHONPIPELINESCRIPT.name(), pipelineScript);
            String pipelineLibScript = getScriptPathWithVersion(classifier.getPythonPipelineLibHdfsPath(), version);
            properties.put(PythonContainerProperty.PYTHONPIPELINELIBFQDN.name(), pipelineLibScript);
            String[] tokens = pipelineLibScript.split("/");
            properties.put(PythonContainerProperty.PYTHONPIPELINELIB.name(), tokens[tokens.length - 1]);

            setLatticeVersion(classifier, properties);
            metadata = JsonUtils.serialize(classifier);
            File metadataFile = new File(dir + "/metadata.json");
            FileUtils.writeStringToFile(metadataFile, metadata, Charset.defaultCharset());
            properties.put(PythonContainerProperty.METADATA_CONTENTS.name(), metadata);
            properties.put(PythonContainerProperty.METADATA.name(), metadataFile.getAbsolutePath());
            properties.put(PythonContainerProperty.CONDA_ENV.name(), emrEnvService.getLatticeCondaEnv());
            if (Boolean.TRUE.equals(useEmr)) {
                properties.put(PythonContainerProperty.WEBHDFS_URL.name(),
                        emrCacheService.getWebHdfsUrl());
            } else {
                properties.put(PythonContainerProperty.WEBHDFS_URL.name(), getWebHdfs());
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private void setLatticeVersion(Classifier classifier, Properties properties) {
        if (classifier == null) {
            return;
        }
        classifier.setProvenanceProperties(classifier.getProvenanceProperties() + //
                " Lattice_Version="
                + properties.getProperty(PythonContainerProperty.VERSION.name()));
    }

    @Override
    public Collection<CopyEntry> getCopyEntries(Properties containerProperties) {
        Collection<CopyEntry> copyEntries = super.getCopyEntries(containerProperties);
        String metadataFilePath = containerProperties
                .getProperty(ContainerProperty.METADATA.name());
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
        String name = classifier.getName();

        if (StringUtils.isEmpty(name)) {
            throw new LedpException(LedpCode.LEDP_10006);
        }

        if (features == null || features.size() == 0) {
            throw new LedpException(LedpCode.LEDP_10002);
        }

        if (targets == null || targets.size() == 0) {
            throw new LedpException(LedpCode.LEDP_10003);
        }

        if (schemaHdfsPath == null) {
            throw new LedpException(LedpCode.LEDP_10000);
        }
        String metadataJson = null;
        try {
            metadataJson = HdfsUtils.getHdfsFileContents(yarnConfiguration, schemaHdfsPath);
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
