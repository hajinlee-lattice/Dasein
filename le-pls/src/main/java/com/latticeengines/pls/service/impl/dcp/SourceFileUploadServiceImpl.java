package com.latticeengines.pls.service.impl.dcp;

import java.io.IOException;
import java.io.InputStream;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.latticeengines.common.exposed.util.GzipUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.dcp.DCPImportRequest;
import com.latticeengines.domain.exposed.dcp.SourceFileInfo;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.dcp.SourceFileUploadService;
import com.latticeengines.proxy.exposed.dcp.DCPProxy;

@Service
public class SourceFileUploadServiceImpl implements SourceFileUploadService {

    private static final Logger log = LoggerFactory.getLogger(SourceFileUploadServiceImpl.class);

    @Inject
    private FileUploadService fileUploadService;

    @Inject
    private DCPProxy dcpProxy;

    @Value("${pls.fileupload.maxupload.bytes}")
    private long maxUploadSize;

    @Override
    public SourceFileInfo uploadFile(String name, String displayName, boolean compressed, EntityType entityType, MultipartFile file) {

        try {
            log.info(String.format("Uploading file %s (displayName=%s, compressed=%s)", name, displayName, compressed));

            if (file.getSize() >= maxUploadSize) {
                throw new LedpException(LedpCode.LEDP_18092, new String[] { Long.toString(maxUploadSize) });
            }

            InputStream stream = file.getInputStream();

            // decompress
            if (compressed) {
                stream = GzipUtils.decompressStream(stream);
            }
            // should we validate file header??
            // validateStream()

            // save file
            SourceFile sourceFile = fileUploadService.uploadFile(name, displayName, entityType, stream);
            return getSourceFileInfo(sourceFile);
        } catch (IOException e) {
            log.error("Cannot get input stream for file: " + name);
            throw new LedpException(LedpCode.LEDP_18053, new String[] { displayName });
        }
    }

    @Override
    public ApplicationId submitSourceImport(String projectId, String sourceId, String sourceFileName) {
        DCPImportRequest request = new DCPImportRequest();
        request.setProjectId(projectId);
        request.setSourceId(sourceId);
        request.setSourceFileName(sourceFileName);
        return dcpProxy.startImport(MultiTenantContext.getShortTenantId(), request);
    }

    private SourceFileInfo getSourceFileInfo(SourceFile sourceFile) {
        if (sourceFile == null) {
            return null;
        }
        SourceFileInfo sourceFileInfo = new SourceFileInfo();
        sourceFileInfo.setName(sourceFile.getName());
        sourceFileInfo.setDisplayName(sourceFile.getDisplayName());
        sourceFileInfo.setFileRows(sourceFile.getFileRows());
        return sourceFileInfo;
    }
}
