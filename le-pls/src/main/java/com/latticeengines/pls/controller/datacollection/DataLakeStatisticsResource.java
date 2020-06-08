package com.latticeengines.pls.controller.datacollection;

import java.util.Map;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "statistics", description = "Statistics of entities in data collection")
@RestController
@RequestMapping("/datacollection/statistics")
public class DataLakeStatisticsResource {

    private final DataLakeService dataLakeService;

    @Inject
    public DataLakeStatisticsResource(DataLakeService dataLakeService) {
        this.dataLakeService = dataLakeService;
    }

    @GetMapping("/cubes")
    @ResponseBody
    @ApiOperation(value = "Get (entity, stats cube) pairs")
    public Map<String, StatsCube> getStatsCubes() {
        return dataLakeService.getStatsCubes();
    }

    @GetMapping("/topn")
    @ResponseBody
    @ApiOperation(value = "Get statistics")
    public TopNTree getTopNTree() {
        return dataLakeService.getTopNTree();
    }

    @GetMapping("/attrs/{entity}/{attribute}")
    @ResponseBody
    @ApiOperation(value = "Get statistics")
    public AttributeStats getAttributeStats(@PathVariable("entity") BusinessEntity entity,
            @PathVariable("attribute") String attribute) {
        return dataLakeService.getAttributeStats(entity, attribute);
    }
}
