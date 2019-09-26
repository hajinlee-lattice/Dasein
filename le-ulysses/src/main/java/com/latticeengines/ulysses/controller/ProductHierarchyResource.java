package com.latticeengines.ulysses.controller;

import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.ulysses.FrontEndResponse;
import com.latticeengines.domain.exposed.ulysses.ProductHierarchy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.objectapi.PeriodTransactionProxy;
import com.latticeengines.ulysses.utils.ProductHierarchyDanteFormatter;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "ProductHierarchy", description = "Common REST resource for product hierarchy data")
@RestController
@RequestMapping("/producthierarchy")
public class ProductHierarchyResource {
    private static final Logger log = LoggerFactory.getLogger(ProductHierarchyResource.class);

    @Inject
    private PeriodTransactionProxy periodTransactionProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Resource(name = ProductHierarchyDanteFormatter.Qualifier)
    private ProductHierarchyDanteFormatter productHierarchyDanteFormatter;

    @GetMapping(value = "/danteformat", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the product hierarchy data for the given account")
    public FrontEndResponse<List<String>> getProductHierarchy() {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();
        try {
            return new FrontEndResponse<>(productHierarchyDanteFormatter
                    .format(JsonUtils.convertList(periodTransactionProxy.getProductHierarchy(customerSpace, //
                            dataCollectionProxy.getActiveVersion(customerSpace)), ProductHierarchy.class)));
        } catch (LedpException le) {
            log.error("Failed to get the product hierarchy data", le);
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            log.error("Failed to get the product hierarchy data", e);
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }
    }
}
