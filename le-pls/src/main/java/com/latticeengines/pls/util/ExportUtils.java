package com.latticeengines.pls.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.latticeengines.app.exposed.download.CustomerSpaceS3FileDownloader;
import com.latticeengines.app.exposed.service.ImportFromS3Service;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.AttributeSet;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public final class ExportUtils {

    protected ExportUtils() {
        throw new UnsupportedOperationException();
    }

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

    public static void main(String[] args) {
        Set<String> accountAttributes = SchemaRepository.getDefaultExportAttributes(BusinessEntity.Account, true)
                .stream().map(InterfaceName::name).collect(Collectors.toSet());
        Set<String> contactAttributes = SchemaRepository.getDefaultExportAttributes(BusinessEntity.Contact, true)
                .stream().map(InterfaceName::name).collect(Collectors.toSet());
        AttributeSet attributeSet = new AttributeSet();
        attributeSet.setDisplayName("displayName");
        Map<String, Set<String>> attributesMap = new HashMap<>();
        attributesMap.put(Category.ACCOUNT_ATTRIBUTES.name(), accountAttributes);
        attributesMap.put(Category.CONTACT_ATTRIBUTES.name(), contactAttributes);
        attributeSet.setAttributesMap(attributesMap);
        System.out.println(JsonUtils.serialize(attributeSet));
    }
}
