package com.latticeengines.datacloudapi.api.controller;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeSource;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import springfox.documentation.annotations.ApiIgnore;

@Api(value = "purge", description = "REST resource for source purge")
@RestController
@RequestMapping("/purge")
public class PurgeResource {
    @RequestMapping(value = "sources", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Get DataCloud sources to purge")
    public List<PurgeSource> getPurgeSources(
            @RequestParam(value = "podid", required = false, defaultValue = "") String hdfsPod) {
        try {
            if (StringUtils.isEmpty(hdfsPod)) {
                hdfsPod = HdfsPodContext.getDefaultHdfsPodId();
                HdfsPodContext.changeHdfsPodId(hdfsPod);
            }
            // Mock a response temporarily for Richa to test
            List<PurgeSource> list = new ArrayList<>();
            List<String> hdfsPaths = new ArrayList<>();
            hdfsPaths.add("/Pods/Production/Services/PropData/Sources/LDCDEV_AccountMasterSample");
            List<String> hiveTables = new ArrayList<>();
            hiveTables.add("ldc_ldcdev_accountmastersample_2017_10_12_01_14_37_utc");
            hiveTables.add("ldc_ldcdev_accountmastersample_2017_10_25_22_40_52_utc");
            PurgeSource src_allvers = new PurgeSource("LDCDEV_AccountMasterSample", null, hdfsPaths, hiveTables,
                    false);
            list.add(src_allvers);
            hdfsPaths.clear();
            hdfsPaths.add(
                    "/Pods/Production/Services/PropData/Sources/LDCDEV_DnBCacheSeedSample/Snapshot/2017-10-27_03-14-49_UTC");
            hdfsPaths.add(
                    "/Pods/Production/Services/PropData/Sources/LDCDEV_DnBCacheSeedSample/Schema/2017-10-27_03-14-49_UTC");
            hiveTables.clear();
            hiveTables.add("ldc_ldcdev_dnbcacheseedsample_2017_10_27_03_14_49_utc");
            PurgeSource src_singlever = new PurgeSource("ldc_ldcdev_dnbcacheseedsample_2017_10_27_03_14_49_utc",
                    null, hdfsPaths, hiveTables, true);
            list.add(src_singlever);
            return list;
        } finally {
            hdfsPod = HdfsPodContext.getDefaultHdfsPodId();
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
    }
}
