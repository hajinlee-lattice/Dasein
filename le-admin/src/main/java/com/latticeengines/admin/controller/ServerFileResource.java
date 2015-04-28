package com.latticeengines.admin.controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PostAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.admin.service.ServerFileService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "files on le-admin server", description = "REST resource for downloading local files from le-admin server")
@RestController
@RequestMapping(value = "/serverfiles")
@PostAuthorize("hasRole('Platform Operations')")
public class ServerFileResource {

    @Autowired
    private ServerFileService serverFileService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of services")
    public void getServerFile(@RequestParam(value = "path") String path,
                              @RequestParam(value = "filename", required = false) String filename,
                              @RequestParam(value = "mimetype",
                                      required = false, defaultValue = "text/plain") String mimeType,
                              HttpServletRequest request, HttpServletResponse response) {
        if (filename == null || filename.equals("")) filename = null;
        serverFileService.downloadFile(request, response, path, filename, mimeType);
    }


}
