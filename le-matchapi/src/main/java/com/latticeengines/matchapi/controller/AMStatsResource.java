package com.latticeengines.matchapi.controller;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.service.AccountMasterStatisticsService;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFactQuery;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributeTree;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "match", description = "REST resource for account master statistics")
@RestController
@RequestMapping("/amstats")
public class AMStatsResource {
    private static final Log log = LogFactory.getLog(AMStatsResource.class);

    private static final ObjectMapper OM = new ObjectMapper();

    @Autowired
    private AccountMasterStatisticsService accountMasterStatisticsService;

    @RequestMapping(value = "/cubes", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get account master statistics cube", response = AccountMasterCube.class)
    private void getCube(@RequestBody AccountMasterFactQuery query, HttpServletResponse response) {
        AccountMasterCube cube = accountMasterStatisticsService.query(query);
        writeToGzipStream(response, cube);
    }

    @RequestMapping(value = "/topattrs", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get categorical attribute tree", response = TopNAttributeTree.class)
    private void getTopAttrTree(HttpServletResponse response) {
        TopNAttributeTree tree = accountMasterStatisticsService.getTopAttrTree();
        writeToGzipStream(response, tree);
    }

    private void writeToGzipStream(HttpServletResponse response, Object output) {
        try {
            OutputStream os = response.getOutputStream();
            response.setHeader("Content-Encoding", "gzip");
            response.setHeader("Content-Type", "application/json");
            GzipCompressorOutputStream gzipOs = new GzipCompressorOutputStream(os);
            OM.writeValue(gzipOs, output);
            gzipOs.flush();
            gzipOs.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}