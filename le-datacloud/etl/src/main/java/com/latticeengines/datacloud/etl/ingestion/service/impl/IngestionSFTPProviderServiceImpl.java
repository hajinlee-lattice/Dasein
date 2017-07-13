package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.SftpUtils;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProviderService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.domain.exposed.datacloud.ingestion.SftpConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;

@Component("ingestionSFTPProviderService")
public class IngestionSFTPProviderServiceImpl implements IngestionProviderService {
    private static Logger log = LoggerFactory.getLogger(IngestionSFTPProviderServiceImpl.class);

    @Autowired
    private IngestionVersionService ingestionVersionService;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected Configuration yarnConfiguration;

    @Override
    public List<String> getMissingFiles(Ingestion ingestion) {
        List<String> result = new ArrayList<String>();
        List<String> targetFiles = getTargetFiles(ingestion);
        List<String> existingFiles = getExistingFiles(ingestion);
        Set<String> existingFilesSet = new HashSet<String>(existingFiles);
        for (String targetFile : targetFiles) {
            if (!existingFilesSet.contains(targetFile)) {
                result.add(targetFile);
                log.info("Found missing file to download: " + targetFile);
            }
        }
        return result;
    }

    private List<String> getTargetFiles(Ingestion ingestion) {
        SftpConfiguration config = (SftpConfiguration) ingestion.getProviderConfiguration();
        String fileNamePattern = config.getFileNamePrefix() + "(.*)" + config.getFileNamePostfix()
                + config.getFileExtension();
        List<String> fileNames = SftpUtils.getFileNames(config, fileNamePattern);
        return ingestionVersionService.getFileNamesOfMostRecentVersions(fileNames, config.getCheckVersion(),
                config.getCheckStrategy(), config.getFileTimestamp());
    }

    private List<String> getExistingFiles(Ingestion ingestion) {
        com.latticeengines.domain.exposed.camille.Path ingestionDir = hdfsPathBuilder
                .constructIngestionDir(ingestion.getIngestionName());
        List<String> result = new ArrayList<String>();
        SftpConfiguration config = (SftpConfiguration) ingestion.getProviderConfiguration();
        final String fileExtension = config.getFileExtension();
        HdfsFileFilter filter = new HdfsFileFilter() {
            @Override
            public boolean accept(FileStatus file) {
                return file.getPath().getName().endsWith(fileExtension);
            }
        };
        try {
            if (HdfsUtils.isDirectory(yarnConfiguration, ingestionDir.toString())) {
                List<String> hdfsFiles = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, ingestionDir.toString(),
                        filter);
                if (!CollectionUtils.isEmpty(hdfsFiles)) {
                    for (String fullName : hdfsFiles) {
                        result.add(new Path(fullName).getName());
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to scan hdfs directory %s", ingestionDir.toString()));
        }
        return result;
    }
}
