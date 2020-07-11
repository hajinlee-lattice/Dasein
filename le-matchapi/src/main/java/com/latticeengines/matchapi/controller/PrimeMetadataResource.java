package com.latticeengines.matchapi.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.datacloud.match.service.PrimeMetadataService;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlock;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockMetadataContainer;
import com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Prime Metadata")
@RestController
@RequestMapping("/prime-metadata")
public class PrimeMetadataResource {

    @Inject
    private PrimeMetadataService primeMetadataService;

    @GetMapping("/elements")
    @ResponseBody
    @ApiOperation(value = "Get all block-level-element tree")
    public List<DataBlock> getBlockElements() {
        return primeMetadataService.getDataBlocks();
    }

    @GetMapping("/blocks")
    @ResponseBody
    @ApiOperation(value = "Get all block metadata")
    public DataBlockMetadataContainer getBlockMetadata() {
        return primeMetadataService.getDataBlockMetadata();
    }

    @GetMapping("/drt-matrix")
    @ResponseBody
    @ApiOperation(value = "Get block - drt matrix")
    public DataBlockEntitlementContainer getBlockDrtMatrix() {
        return primeMetadataService.getBaseEntitlement();
    }

    @GetMapping("/columns")
    public List<PrimeColumn> getPrimeColumns(@RequestParam("elementIds") List<String> elementIds) {
        return primeMetadataService.getPrimeColumns(elementIds);
    }

}
