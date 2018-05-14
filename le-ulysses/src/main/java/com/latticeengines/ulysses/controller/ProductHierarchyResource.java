package com.latticeengines.ulysses.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.inject.Inject;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.ulysses.FrontEndResponse;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.objectapi.PeriodTransactionProxy;
import com.latticeengines.ulysses.utils.ProductHierarchyDanteFormatter;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "ProductHierarchy", description = "Common REST resource for product hierarchy data")
@RestController
@RequestMapping("/producthierarchy")
public class ProductHierarchyResource {

    @Inject
    private PeriodTransactionProxy periodTransactionProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    @Qualifier(ProductHierarchyDanteFormatter.Qualifier)
    private ProductHierarchyDanteFormatter productHierarchyDanteFormatter;

    @RequestMapping(value = "/danteformat", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the purchase history data for the given account")
    public FrontEndResponse<List<String>> getProductHierarchy() {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();
        try {
            return new FrontEndResponse<>(productHierarchyDanteFormatter.format(periodTransactionProxy
                    .getProductHierarchy(customerSpace, dataCollectionProxy.getActiveVersion(customerSpace))));
        } catch (LedpException le) {
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }
    }
}
