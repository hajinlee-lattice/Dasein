package com.latticeengines.pls.controller;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.pls.service.FileDownloadService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "download")
@RestController
@RequestMapping("/filedownloads")
public class FileDownloadController {

    private static final Logger log = LoggerFactory.getLogger(FileDownloadController.class);

    @Inject
    private FileDownloadService fileDownloadService;

    @GetMapping(value = "/{token}")
    @ResponseBody
    @ApiOperation("Download process/import/error files by uploadId")
    public void downloadByToken(@PathVariable String token,
                                HttpServletRequest request, HttpServletResponse response) throws Exception {
        try {
            fileDownloadService.downloadByToken(token, request, response);
        } catch (Exception e) {
            log.error("failed to download config: {}", e.getMessage());
            throw e;
        }
    }

}
