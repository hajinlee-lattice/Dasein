package com.latticeengines.pls.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.datacloud.manage.DataBlock;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockMetadataContainer;
import com.latticeengines.proxy.exposed.matchapi.PrimeMetadataProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Data Block")
@RestController
@RequestMapping("/data-blocks")
public class DataBlockResource {

    @Inject
    private PrimeMetadataProxy primeMetadataProxy;

    @GetMapping("/metadata")
    @ResponseBody
    @ApiOperation(value = "Get block metadata")
    public DataBlockMetadataContainer getBlockMetadata() {
        return primeMetadataProxy.getBlockMetadata();
    }

    @GetMapping("/elements")
    @ResponseBody
    @ApiOperation(value = "Get block-level-element tree")
    public List<DataBlock> getBlocks(@RequestParam(value = "blockIds", required = false) List<String> blockIds) {
        return primeMetadataProxy.getBlockElements(blockIds);
    }

    @GetMapping("/entitlement")
    @ResponseBody
    @ApiOperation(value = "Get block drt entitlement")
    @PreAuthorize("hasRole('Edit_DCP_Projects')")
    public DataBlockEntitlementContainer getEntitlement() {
        return primeMetadataProxy.getBlockDrtMatrix();
    }

}
