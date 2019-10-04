package com.latticeengines.ulysses.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.controller.PrimaryAttributeResourceBase;

import io.swagger.annotations.Api;

/**
 * This api is deprecated. Start using resource "/primary-attributes" which is
 * derived from le-app
 *
 */
@Api(value = "attributes", description = "REST resource for attribute configurations")
@RestController
@RequestMapping("/attributes")
@ResponseBody
@Deprecated
public class AttributeResource extends PrimaryAttributeResourceBase {
    // Need to refactor this implementation to support PLS-10330
    // Refactored the code to le-app. refer to PrimaryAttributeResource in le-app
}
