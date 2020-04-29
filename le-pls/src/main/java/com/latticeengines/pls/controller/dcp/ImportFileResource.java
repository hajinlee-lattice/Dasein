package com.latticeengines.pls.controller.dcp;

import javax.inject.Inject;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.latticeengines.domain.exposed.dcp.SourceFileInfo;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.UIActionException;
import com.latticeengines.domain.exposed.pls.frontend.Status;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.pls.frontend.View;
import com.latticeengines.pls.service.dcp.SourceFileUploadService;
import com.latticeengines.pls.service.impl.GraphDependencyToUIActionUtil;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "importfile", description = "REST resource for import files for modeling")
@RestController
@RequestMapping("/importfile")
@PreAuthorize("hasRole('View_DCP_Projects')")
public class ImportFileResource {

    private static final Logger log = LoggerFactory.getLogger(ImportFileResource.class);
    public static final String UPLOAD_FILE_ERROR_TITLE = "Error In File Uploading.";

    @Inject
    private SourceFileUploadService sourceFileUploadService;

    @Value("${pls.fileupload.maxupload.bytes}")
    private long maxUploadSize;

    @Inject
    private GraphDependencyToUIActionUtil graphDependencyToUIActionUtil;

    @PostMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Upload a file")
    public SourceFileInfo uploadFile(
            @RequestParam("name") String csvFileName,
            @RequestParam("file") MultipartFile file) {
        SourceFileInfo response = uploadFile("file_" + DateTime.now().getMillis() + ".csv", csvFileName, file);
        return response;
    }

    private SourceFileInfo uploadFile(String fileName, String csvFileName, MultipartFile file) {
        try {
            if (file.getSize() >= maxUploadSize) {
                throw new LedpException(LedpCode.LEDP_18092, new String[] { Long.toString(maxUploadSize) });
            }

            return sourceFileUploadService.uploadFile(fileName, csvFileName, false, null, file);
        } catch (LedpException ledp) {
            UIAction action = graphDependencyToUIActionUtil.generateUIAction(UPLOAD_FILE_ERROR_TITLE, View.Banner,
                    Status.Error, ledp.getMessage());
            throw new UIActionException(action, ledp.getCode());
        }
    }

}
