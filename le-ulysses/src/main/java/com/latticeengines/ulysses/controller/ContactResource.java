package com.latticeengines.ulysses.controller;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.query.DataPage;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Contacts", description = "Common REST resource to lookup contacts")
@RestController
@RequestMapping("/contacts")
public class ContactResource {

    private static final String commonResourcePath = "metadata/";
    private static final String tempContactResponsePath = "tempContactResponse.json";

    @GetMapping(value = "/accounts/{accountId}")
    @ResponseBody
    @ApiOperation(value = "Get all contacts for the given account id")
    public DataPage getContactsByAccountId(@PathVariable String accountId) {
        String tempContactResponseDocument = getStaticDocument(commonResourcePath + tempContactResponsePath);
        return JsonUtils.deserialize(tempContactResponseDocument, DataPage.class);
    }

    private String getStaticDocument(String documentPath) {
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            InputStream tableRegistryStream = classLoader.getResourceAsStream(documentPath);
            return StreamUtils.copyToString(tableRegistryStream, Charset.defaultCharset());
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_10011, e,
                    new String[] { documentPath.replace(commonResourcePath, "") });
        }
    }
}
