package com.latticeengines.pls.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.pls.service.MetadataFileUploadService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("metadataFileUploadService")
public class MetadataFileUploadServiceImpl implements MetadataFileUploadService {

    private static final Logger log = LoggerFactory.getLogger(MetadataFileUploadServiceImpl.class);

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private MetadataProxy metadataProxy;

    @Override
    public String uploadFile(String urlToken, String moduleName, String artifactName, InputStream inputStream) {
        ArtifactType artifactType = ArtifactType.getArtifactTypeByUrlToken(urlToken);

        if (artifactType == null) {
            throw new LedpException(LedpCode.LEDP_18090, new String[] { urlToken });
        }
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Path path = PathBuilder.buildMetadataPathForArtifactType(CamilleEnvironment.getPodId(), //
                customerSpace, moduleName, artifactType);
        String hdfsPath = String.format("%s/%s.%s", path.toString(), artifactName, artifactType.getFileType());
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, hdfsPath)) {
                throw new LedpException(LedpCode.LEDP_18091, new String[] { artifactType.getCode(), artifactName,
                        moduleName });
            }
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, inputStream, hdfsPath);
            metadataProxy.validateArtifact(customerSpace.toString(), artifactType, hdfsPath);
            Artifact artifact = new Artifact();
            artifact.setPath(hdfsPath);
            artifact.setArtifactType(artifactType);
            metadataProxy.createArtifact(customerSpace.toString(), moduleName, artifactName, artifact);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return hdfsPath;
    }

    @Override
    public List<Module> getModules() {
        List<Module> modules = new ArrayList<>();
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Path path = PathBuilder.buildMetadataPath(CamilleEnvironment.getPodId(), customerSpace);
        try {
            List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, path.toString());

            for (String filePath : files) {
                Module module = new Module();
                module.setName(new org.apache.hadoop.fs.Path(filePath).getName());
                modules.add(module);
            }
        } catch (IOException e) {
            log.warn(e.getMessage());
            return new ArrayList<>();
        }
        return modules;
    }

    @Override
    public List<Artifact> getArtifacts(String moduleName, ArtifactType artifactType) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        String path = String.format("%s/%s/%s", //
                PathBuilder.buildMetadataPath(CamilleEnvironment.getPodId(), customerSpace).toString(), //
                moduleName, //
                artifactType.getPathToken());
        List<Artifact> artifacts = new ArrayList<>();
        try {
            List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, path);

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

    @Override
    public List<Artifact> getArtifacts(String moduleName, String urlToken) {
        ArtifactType artifactType = ArtifactType.getArtifactTypeByUrlToken(urlToken);

        if (artifactType == null) {
            throw new LedpException(LedpCode.LEDP_18090, new String[] { urlToken });
        }
        return getArtifacts(moduleName, artifactType);
    }

}
