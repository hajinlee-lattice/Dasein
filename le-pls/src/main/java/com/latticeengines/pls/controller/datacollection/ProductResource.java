package com.latticeengines.pls.controller.datacollection;

import javax.inject.Inject;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "products", description = "REST resource for serving data about products")
@RestController
@RequestMapping("/products")
@PreAuthorize("hasRole('View_PLS_CDL_Data')")
public class ProductResource extends BaseFrontEndEntityResource {

    @Inject
    public ProductResource(EntityProxy entityProxy, SegmentProxy segmentProxy) {
        super(entityProxy, segmentProxy);
    }

    @RequestMapping(value = "/data", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Retrieve all the products")
    public DataPage getData( //
            @ApiParam(value = "Offset for pagination of products") //
            @RequestParam(value = "offset", required = false)  //
            Integer offset, //
            @ApiParam(value = "Maximum number of products in page")//
            @RequestParam(value = "max", required = false)//
            Integer max) {
        FrontEndQuery query = new FrontEndQuery();
        if (offset != null || max != null) {
            offset = offset == null ? 0 : offset;
            query.setPageFilter(new PageFilter(offset, max));
        }
        return super.getData(query);
    }

    @Override
    BusinessEntity getMainEntity() {
        return BusinessEntity.Product;
    }

}
