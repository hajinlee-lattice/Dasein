package com.latticeengines.pls.util;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.latticeengines.app.exposed.download.CustomerSpaceS3FileDownloader;
import com.latticeengines.app.exposed.service.ImportFromS3Service;
import com.latticeengines.baton.exposed.service.BatonService;

public final class ExportUtils {

    public static void downloadS3ExportFile(String filePath, String fileName, String mediaType, HttpServletRequest request,
                                            HttpServletResponse response, ImportFromS3Service importFromS3Service, BatonService batonService) {
        String fileNameIndownloader = fileName;
        if (fileNameIndownloader.endsWith(".gz")) {
            fileNameIndownloader = fileNameIndownloader.substring(0, fileNameIndownloader.length() - 3);
        }
        response.setHeader("Content-Encoding", "gzip");
        CustomerSpaceS3FileDownloader.S3FileDownloadBuilder builder = new CustomerSpaceS3FileDownloader.S3FileDownloadBuilder();
        builder.setMimeType(mediaType).setFilePath(filePath).setFileName(fileNameIndownloader).setImportFromS3Service(importFromS3Service).setBatonService(batonService);
        CustomerSpaceS3FileDownloader customerSpaceS3FileDownloader = new CustomerSpaceS3FileDownloader(builder);
        customerSpaceS3FileDownloader.downloadFile(request, response);
    }
}
