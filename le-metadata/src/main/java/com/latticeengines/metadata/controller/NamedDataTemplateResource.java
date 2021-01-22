package com.latticeengines.metadata.controller;

import javax.inject.Inject;

import org.reactivestreams.Publisher;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.metadata.service.NamedDataTemplateService;

import io.swagger.annotations.Api;

@Api(value = "NamedDataTemplate", description = "REST resource for data template")
@RestController
@RequestMapping("/datatemplate")
public class NamedDataTemplateResource {

    @Inject
    private NamedDataTemplateService namedDataTemplateService;

    @GetMapping("/{dtName}/namespace/{namespace}/count")
    public long countSchema( //
            @PathVariable(name = "dtName") String dtName, //
            @PathVariable(name = "namespace") String[] namespace) {
        return namedDataTemplateService.getSchemaCount(dtName, namespace);
    }

    @GetMapping("/{dtName}/namespace/{namespace}")
    public Publisher<ColumnMetadata> getSchema( //
            @PathVariable(name = "dtName") String dtName, //
            @PathVariable(name = "namespace") String[] namespace, //
            @RequestParam(name = "unordered", required = false) Boolean unordered) {
        if (Boolean.TRUE.equals(unordered)) {
            return namedDataTemplateService.getUnorderedSchema(dtName, namespace);
        } else {
            return namedDataTemplateService.getSchema(dtName, namespace);
        }
    }

}
