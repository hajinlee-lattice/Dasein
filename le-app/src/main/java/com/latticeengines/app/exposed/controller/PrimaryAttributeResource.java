package com.latticeengines.app.exposed.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;

@Api(value = "primary-attributes", description = "REST resource for primary attribute configurations")
@RestController
@RequestMapping("/primary-attributes")
@ResponseBody
public class PrimaryAttributeResource extends PrimaryAttributeResourceBase {

    // This class is moved from le-ulysses as the same resource needs to be referenced from PLS as well.
    // Unfortunately, I cannot use the ulysses @RequestMapping("/attributes"), as it was already used in PLS service for attribute-management
    // As we cannot refactor the downstream apps like BIS, Eloqua integrations, I need to refactor it to support backward comapatibility of existing usecases
    // So, moved the code to base class and provided different endpoints for PLS vs ULYSSES to support both GA and OAuth respectively.
    // Marked the ulysses API as deprecated
}
