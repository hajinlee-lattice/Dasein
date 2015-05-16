package com.latticeengines.admin.controller;

import org.springframework.security.access.prepost.PostAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.wordnik.swagger.annotations.Api;

@Api(value = "files on le-admin server", description = "REST resource for downloading local files from le-admin server")
@RestController
@RequestMapping(value = "/serverfiles")
@PostAuthorize("hasRole('Platform Operations')")
public class ServerFileResource {

//    @Autowired
//    private ServerFileService serverFileService;

//    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
//    @ResponseBody
//    @ApiOperation(value = "Get list of services")
//    public void getServerFile(@RequestParam(value = "path") String path,
//                              @RequestParam(value = "filename", required = false) String filename,
//                              @RequestParam(value = "mimetype",
//                                      required = false, defaultValue = "text/plain") String mimeType,
//                              HttpServletRequest request, HttpServletResponse response) {
//        if (filename == null || filename.equals("")) filename = null;
//        serverFileService.downloadFile(request, response, path, filename, mimeType);
//    }



}
