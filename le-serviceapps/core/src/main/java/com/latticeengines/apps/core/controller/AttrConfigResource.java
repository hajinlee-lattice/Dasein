package com.latticeengines.apps.core.controller;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.AttributeSet;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigCategoryOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "AttrConfig", description = "REST resource for default LP attribute config.")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/attrconfig")
public class AttrConfigResource {

    @Inject
    private AttrConfigService attrConfigService;

    @GetMapping(value = "/entities/{entity}")
    @ResponseBody
    @ApiOperation("get cdl attribute config request")
    public AttrConfigRequest getAttrConfigByEntity(@PathVariable String customerSpace,
            @PathVariable BusinessEntity entity,
            @RequestParam(value = "render", required = false, defaultValue = "true") boolean render) {
        AttrConfigRequest request = new AttrConfigRequest();
        List<AttrConfig> attrConfigs = attrConfigService.getRenderedList(entity, render);
        request.setAttrConfigs(attrConfigs);
        return request;
    }

    @GetMapping(value = "/custom-displaynames")
    @ResponseBody
    @ApiOperation("get cdl attribute customized display names")
    public Map<BusinessEntity, List<AttrConfig>> getCustomDisplayNames(@PathVariable String customerSpace) {
        return attrConfigService.findAllHaveCustomDisplayNameByTenantId(MultiTenantContext.getShortTenantId());
    }

    @GetMapping(value = "/categories/{categoryName}")
    @ResponseBody
    @ApiOperation("get cdl attribute config request")
    public AttrConfigRequest getAttrConfigByCategory(@PathVariable String customerSpace,
            @PathVariable String categoryName, @RequestParam(value = "attributeSetName", required = false) String attributeSetName) {
        AttrConfigRequest request = new AttrConfigRequest();
        Category category = resolveCategory(categoryName);
        List<AttrConfig> attrConfigs = attrConfigService.getRenderedList(category, attributeSetName);
        request.setAttrConfigs(attrConfigs);
        return request;
    }

    @GetMapping(value = "/properties/{propertyName}")
    @ResponseBody
    @ApiOperation("get cdl attribute config request by property")
    public AttrConfigRequest getAttrConfigByProperty(@PathVariable String customerSpace,
            @PathVariable String propertyName,
            @RequestParam(value = "enabled", required = false, defaultValue = "true") Boolean enabled) {
        AttrConfigRequest request = new AttrConfigRequest();
        List<AttrConfig> attrConfigs = attrConfigService.getRenderedList(propertyName, enabled);
        request.setAttrConfigs(attrConfigs);
        return request;
    }

    @PostMapping(value = "/overview")
    @ResponseBody
    public Map<String, AttrConfigCategoryOverview<?>> getAttrConfigOverview(@PathVariable String customerSpace,
                                                                            @RequestParam(value = "category", required = false) List<String> categoryNames, //
                                                                            @RequestParam(value = "activeOnly", required = false, defaultValue = "0") boolean activeOnly, //
                                                                            @RequestParam(value = "attributeSetName", required = false) String attributeSetName,
                                                                            @RequestBody List<String> propertyNames) {
        List<Category> categories = categoryNames != null
                ? categoryNames.stream().map(this::resolveCategory).collect(Collectors.toList())
                : Arrays.stream(Category.values()).filter(category -> !category.isHiddenFromUi())
                .collect(Collectors.toList());
        return attrConfigService.getAttrConfigOverview(categories, propertyNames, activeOnly, attributeSetName);
    }

    @PostMapping(value = "")
    @ResponseBody
    @ApiOperation("save cdl attribute config request")
    public AttrConfigRequest saveAttrConfig(@PathVariable String customerSpace, @RequestBody AttrConfigRequest request,
            @RequestParam(value = "mode", required = true) AttrConfigUpdateMode mode) {
        request.fixJsonDeserialization();
        return attrConfigService.saveRequest(request, mode);
    }

    @DeleteMapping(value = "")
    @ResponseBody
    @ApiOperation("delete attribute for tenant")
    public void removeAttrConfig(@PathVariable String customerSpace,
            @RequestParam(required = false, value = "entity") BusinessEntity entity) {
        if (entity == null) {
            attrConfigService.removeAttrConfig(MultiTenantContext.getShortTenantId());
        } else {
            attrConfigService.removeAttrConfigForEntity(MultiTenantContext.getShortTenantId(), entity);
        }
    }

    // Deprecated before M23
    @PostMapping(value = "/validate")
    @ResponseBody
    @ApiOperation("put cdl attribute config request")
    public AttrConfigRequest validateAttrConfig(@PathVariable String customerSpace,
            @RequestBody AttrConfigRequest request,
            @RequestParam(value = "mode", required = true) AttrConfigUpdateMode mode) {
        request.fixJsonDeserialization();
        return attrConfigService.validateRequest(request, mode);
    }

    private Category resolveCategory(String categoryName) {
        Category category = Category.fromName(categoryName);
        if (category == null) {
            throw new IllegalArgumentException("Cannot parse category " + categoryName);
        }
        return category;
    }

    @GetMapping(value = "/attributeset/name/{name}")
    @ApiOperation(value = "Get attribute set by name")
    public AttributeSet getAttributeSetByName(@PathVariable String customerSpace, @PathVariable(value = "name") String name) {
        return attrConfigService.getAttributeSetByName(name);
    }

    @GetMapping(value = "/attributeset")
    @ApiOperation(value = "Get attribute set list")
    public List<AttributeSet> getAttributeSets(@PathVariable String customerSpace) {
        return attrConfigService.getAttributeSets();
    }

    @PostMapping(value = "/attributeset/clone")
    @ApiOperation(value = "Createa or update attribute set")
    public AttributeSet cloneAttributeSet(@PathVariable String customerSpace,
                                          @RequestParam(required = false, value = "attributeSetName") String attributeSetName,
                                          @RequestBody AttributeSet attributeSet) {
        return attrConfigService.cloneAttributeSet(attributeSetName, attributeSet);
    }

    @PutMapping(value = "/attributeset/update")
    @ApiOperation(value = "update attribute set")
    public AttributeSet updateAttributeSet(@PathVariable String customerSpace, @RequestBody AttributeSet attributeSet) {
        return attrConfigService.updateAttributeSet(attributeSet);
    }

    @DeleteMapping(value = "/attributeset/name/{name}")
    @ApiOperation(value = "Delete attribute set")
    public Boolean deleteAttributeSetByName(@PathVariable String customerSpace, @PathVariable("name") String name) {
        attrConfigService.deleteAttributeSetByName(name);
        return true;
    }

}
