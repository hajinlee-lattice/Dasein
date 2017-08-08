package com.latticeengines.pls.controller.datacollection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.pls.service.DataLakeService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "statistics", description = "Statistics of entities in data collection")
@RestController
@RequestMapping("/datacollection/statistics")
public class DataLakeStatisticsResource {

    @Autowired
    private DataLakeService dataLakeService;

    @RequestMapping(value = "/cube", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get flat attribute stats map")
    public StatsCube getStatsCube() {
        return dataLakeService.getStatsCube();
    }

    @RequestMapping(value = "/topn", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get statistics")
    public TopNTree getTopNTree(@RequestParam(value = "topbkt", required = false) Boolean includeTopBkt) {
        return dataLakeService.getTopNTree(Boolean.TRUE.equals(includeTopBkt));
    }
}
