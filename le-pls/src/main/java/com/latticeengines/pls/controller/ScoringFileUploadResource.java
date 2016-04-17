package com.latticeengines.pls.controller;

import java.io.IOException;
import java.io.InputStream;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.latticeengines.common.exposed.util.ZipUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.pls.service.FileUploadService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "scores/fileuploads", description = "REST resource for uploading csv files for scoring")
@RestController
@RequestMapping("/scores/fileuploads")
@PreAuthorize("hasRole('View_PLS_Data')")
public class ScoringFileUploadResource {

    @Autowired
    private FileUploadService fileUploadService;

    @Value("${pls.fileupload.maxupload.bytes}")
    private long maxUploadSize;

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upload a file")
    public SourceFile uploadFile( //
            @RequestParam(value = "displayName") String displayName, //
            @RequestParam(value = "modelId") String modelId, //
            @RequestParam(value = "compressed", required = false) boolean compressed, //
            @RequestParam("file") MultipartFile file) {
        try {
            if (file.getSize() >= maxUploadSize) {
                throw new LedpException(LedpCode.LEDP_18092, new String[] { Long.toString(maxUploadSize) });
            }

            InputStream stream = file.getInputStream();
            if (compressed) {
                stream = ZipUtils.decompressStream(stream);
            }

            // TODO Validate metadata
            // TODO Add SourceFile provenance mechanism and add modelId

            return fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                    SchemaInterpretation.TestingData, displayName, stream);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18053, new String[] { displayName });
        }
    }

    @RequestMapping(value = "{fileName}/import/errors", method = RequestMethod.GET, produces = "text/plain")
    @ResponseBody
    @ApiOperation(value = "Retrieve file import errors")
    public void getImportErrors(@PathVariable String fileName, HttpServletResponse response) {
        try {
            InputStream is = fileUploadService.getImportErrorStream(fileName);
            response.setContentType("text/plain");
            response.setHeader("Content-Disposition", String.format("attachment; filename=\"%s\"", "errors.csv"));
            IOUtils.copy(is, response.getOutputStream());
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18093, e);
        }
    }
}
