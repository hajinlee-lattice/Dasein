package com.latticeengines.dataplatform.client.yarn;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.yarn.fs.LocalResourcesFactoryBean;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.exposed.yarn.client.ContainerProperty;
import com.latticeengines.dataplatform.exposed.yarn.client.DefaultYarnClientCustomization;
import com.latticeengines.dataplatform.runtime.python.PythonContainerProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.Field;
import com.latticeengines.domain.exposed.swlib.SoftwarePackage;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;

@Component("pythonClientCustomization")
public class PythonClientCustomization extends DefaultYarnClientCustomization {
    
    private static final Log log = LogFactory.getLog(PythonClientCustomization.class);
    
    public PythonClientCustomization() {
        super(null, null, null, null, null);
    }

    @Autowired
    public PythonClientCustomization(Configuration yarnConfiguration, //
            VersionManager versionManager, //
            SoftwareLibraryService softwareLibraryService, //
            @Value("${dataplatform.yarn.job.basedir}") String hdfsJobBaseDir, //
            @Value("${dataplatform.fs.web.defaultFS}") String webHdfs) {
        super(yarnConfiguration, versionManager, softwareLibraryService, hdfsJobBaseDir, webHdfs);
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
            String pipelineScript = classifier.getPythonPipelineScriptHdfsPath();
            String pipelineLibScript = classifier.getPythonPipelineLibHdfsPath();
            properties.put(PythonContainerProperty.PYTHONPIPELINESCRIPT.name(), pipelineScript);
            properties.put(PythonContainerProperty.PYTHONPIPELINELIBFQDN.name(), pipelineLibScript);
            String[] tokens = pipelineLibScript.split("/");
            properties.put(PythonContainerProperty.PYTHONPIPELINELIB.name(), tokens[tokens.length - 1]);
            properties.put(PythonContainerProperty.SCHEMA.name(), classifier.getSchemaHdfsPath());
            properties.put(PythonContainerProperty.DATAPROFILE.name(), classifier.getDataProfileHdfsPath());
            properties.put(PythonContainerProperty.CONFIGMETADATA.name(), classifier.getConfigMetadataHdfsPath());

            
            properties.put(PythonContainerProperty.VERSION.name(), versionManager.getCurrentVersion());
            properties.put(PythonContainerProperty.SWLIBARTIFACT.name(), getSwlibArtifactHdfsPath(classifier));
            setLatticeVersion(classifier, properties);
            metadata = JsonUtils.serialize(classifier);
            File metadataFile = new File(dir + "/metadata.json");
            FileUtils.writeStringToFile(metadataFile, metadata);
            properties.put(PythonContainerProperty.METADATA_CONTENTS.name(), metadata);
            properties.put(PythonContainerProperty.METADATA.name(), metadataFile.getAbsolutePath());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
    
    private void setLatticeVersion(Classifier classifier, Properties properties) {
        if (classifier == null) {
            return;
        }
        classifier.setProvenanceProperties(classifier.getProvenanceProperties() + //
                " Lattice_Version=" + properties.getProperty(PythonContainerProperty.VERSION.name()));
    }
    
    @VisibleForTesting
    String getSwlibArtifactHdfsPath(Classifier classifier) {
        if (classifier == null) {
            return "";
        }
        String provenanceProperties = classifier.getProvenanceProperties();
        
        try {
            if (!StringUtils.isEmpty(provenanceProperties)) {
                String[] properties = provenanceProperties.split("\\s");
                
                String module = null;
                String groupId = null;
                String artifactId = null;
                String version = null;
                for (String property : properties) {
                    property = property.trim();
                    if (property.startsWith("swlib.module=")) {
                        module = property.split("=")[1];
                    } else if (property.startsWith("swlib.artifact_id=")) {
                        artifactId = property.split("=")[1];
                    } else if (property.startsWith("swlib.group_id=")) {
                        groupId = property.split("=")[1];
                    } else if (property.startsWith("swlib.version=")) {
                        version = property.split("=")[1];
                    }
                }
                List<SoftwarePackage> packages = softwareLibraryService.getInstalledPackagesByVersion( //
                        module, versionManager.getCurrentVersion());
                if (StringUtils.isEmpty(versionManager.getCurrentVersion())) {
                    packages = softwareLibraryService.getLatestInstalledPackages(module);
                }
                
                if (packages.size() == 0) {
                    throw new Exception("No software library modules.");
                }
                
                // if any of the required metadata is null, then return the first swlib package
                if (module == null || groupId == null || artifactId == null || version == null) {
                    SoftwarePackage pkg = packages.get(0);
                    provenanceProperties = String.format("swlib.artifact_id=%s swlib.version=%s", //
                            pkg.getArtifactId(), pkg.getVersion());
                    classifier.setProvenanceProperties(classifier.getProvenanceProperties() + " " + provenanceProperties);
                    return SoftwareLibraryService.TOPLEVELPATH + "/" + pkg.getHdfsPath();
                }
                
                for (SoftwarePackage pkg : packages) {
                    if (version.equals("latest")) {
                        version = pkg.getVersion();
                    }
                    if (artifactId.equals(pkg.getArtifactId()) //
                            && groupId.equals(pkg.getGroupId()) //
                            && module.equals(pkg.getModule()) //
                            && version.equals(pkg.getVersion())) {
                        resetClassifierWithProperProvenanceProperties(classifier, pkg);
                        return SoftwareLibraryService.TOPLEVELPATH + "/" + pkg.getHdfsPath();
                    }
                }
            }
        } catch (Exception e) {
            log.warn(LedpException.buildMessage(LedpCode.LEDP_00000, new String[] {}));
            return classifier.getPythonPipelineLibHdfsPath();
        }
        return classifier.getPythonPipelineLibHdfsPath();
    }
    
    void resetClassifierWithProperProvenanceProperties(Classifier classifier, SoftwarePackage pkg) {
        String[] properties = classifier.getProvenanceProperties().split("\\s");
        
        List<String> finalProvenanceProperties = new ArrayList<>();
        for (String property : properties) {
            if (!property.startsWith("swlib.")) {
                finalProvenanceProperties.add(property);
            }
        }
        finalProvenanceProperties.add("swlib.artifact_id=" + pkg.getArtifactId());
        finalProvenanceProperties.add("swlib.group_id=" + pkg.getGroupId());
        finalProvenanceProperties.add("swlib.version=" + pkg.getVersion());
        finalProvenanceProperties.add("swlib.module=" + pkg.getModule());
        String provenanceProperties = StringUtils.join(finalProvenanceProperties, " ");
        classifier.setProvenanceProperties(provenanceProperties);
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
