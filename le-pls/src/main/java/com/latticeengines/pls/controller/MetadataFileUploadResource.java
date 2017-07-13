package com.latticeengines.pls.controller;

import java.io.InputStream;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.latticeengines.common.exposed.util.GzipUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.pls.service.MetadataFileUploadService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metadatauploads", description = "REST resource for uploading metadata files")
@RestController
@RequestMapping("/metadatauploads")
public class MetadataFileUploadResource {

    private static final Logger log = LoggerFactory.getLogger(MetadataFileUploadResource.class);

    @Autowired
    private MetadataFileUploadService metadataFileUploadService;

    @Value("${pls.fileupload.maxupload.bytes}")
    private long maxUploadSize;

    @RequestMapping(value = "/modules/{moduleName}/{metadataType}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upload a metadata file")
    public ResponseDocument<String> uploadFile(@PathVariable String metadataType, //
            @PathVariable String moduleName, //
            @RequestParam("artifactName") String artifactName, //
            @RequestParam(value = "compressed", required = false) boolean compressed, //
            @RequestParam("metadataFile") MultipartFile metadataFile) {
        try {
            log.info(String.format("Uploading file (moduleName=%s, artifactName=%s, compressed=%s)", moduleName, artifactName,
                    compressed));
            if (metadataFile.getSize() >= maxUploadSize) {
                throw new LedpException(LedpCode.LEDP_18092, new String[] { Long.toString(maxUploadSize) });
            }

            InputStream stream = metadataFile.getInputStream();

            if (compressed) {
                stream = GzipUtils.decompressStream(stream);
            }

            return ResponseDocument.successResponse(metadataFileUploadService.uploadFile(metadataType, //
                    moduleName, artifactName, //
                    stream));
        } catch (Exception e) {
            if (e instanceof LedpException) {
                throw (LedpException) e;
            }
            log.error(e.getMessage(), e);
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/modules", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get list of modules")
    public List<Module> getModules() {
        return metadataFileUploadService.getModules();
    }

}
