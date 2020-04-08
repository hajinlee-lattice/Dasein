package com.latticeengines.pls.service.impl;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HashUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.FileDownload;
import com.latticeengines.domain.exposed.pls.FileDownloadConfig;
import com.latticeengines.pls.entitymanager.FileDownloadEntityMgr;
import com.latticeengines.pls.service.AbstractFileDownloadService;
import com.latticeengines.pls.service.FileDownloadService;

@Component("fileDownloadService")
public class FileDownloadServiceImpl implements FileDownloadService {

    @Inject
    private FileDownloadEntityMgr fileDownloadEntityMgr;

    @Override
    public String generateDownload(FileDownloadConfig fileDownloadConfig) {
        FileDownload fileDownload = new FileDownload();
        String token = HashUtils.getMD5CheckSum(UUID.randomUUID().toString());
        fileDownload.setFileDownloadConfig(fileDownloadConfig);
        fileDownload.setTenant(MultiTenantContext.getTenant());
        fileDownload.setToken(token);
        fileDownload.setTtl(10);
        fileDownload.setCreation(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
        fileDownloadEntityMgr.create(fileDownload);
        return token;
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public void downloadByToken(String token, HttpServletRequest request, HttpServletResponse response)
            throws Exception {
        FileDownload fileDownload = fileDownloadEntityMgr.findByToken(token);
        if (fileDownload == null) {
            throw new RuntimeException("no token exists");
        }
        long create = fileDownload.getCreation();
        int ttlInMinute = fileDownload.getTtl();
        long now = LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        if (create + TimeUnit.MINUTES.toMillis(ttlInMinute) < now) {
            throw new RuntimeException("token time out");
        }
        FileDownloadConfig config = fileDownload.getFileDownloadConfig();
        MultiTenantContext.setTenant(fileDownload.getTenant());
        AbstractFileDownloadService fileDownloadService =
                AbstractFileDownloadService.getDownloadService(config.getClass());
        fileDownloadService.downloadByConfig(config, request, response);

    }
}
