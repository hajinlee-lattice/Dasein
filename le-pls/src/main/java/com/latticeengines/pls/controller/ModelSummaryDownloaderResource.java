package com.latticeengines.pls.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.pls.entitymanager.ModelSummaryDownloaderEntityMgr;
import io.swagger.annotations.Api;

@Api(value = "modelsummarydownload", description = "REST resource for model summary downloader")
@RestController
@RequestMapping("/msdownload")
public class ModelSummaryDownloaderResource {

    @Autowired
    private ModelSummaryDownloaderEntityMgr modelSummaryDownloaderEntityMgr;

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    public String startDownload() {
        return modelSummaryDownloaderEntityMgr.downloadModel();
    }

}
