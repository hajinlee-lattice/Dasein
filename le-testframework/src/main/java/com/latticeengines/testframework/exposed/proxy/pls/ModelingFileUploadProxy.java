package com.latticeengines.testframework.exposed.proxy.pls;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;

@Service("modelingFileUploadProxy")
public class ModelingFileUploadProxy extends PlsRestApiProxyBase {

    public ModelingFileUploadProxy() {
        super("pls/models/uploadfile");
    }

    @SuppressWarnings("unchecked")
    public SourceFile uploadFile(String fileName, boolean compressed, String csvFileName, SchemaInterpretation schemaInterpretation, //
                                 String entity, Resource fileResource){
        List<Object> args = new ArrayList<>();
        args.add(fileName);
        args.add(csvFileName);
        args.add(compressed);
        String urlPattern = "/?fileName={fileName}&displayName={csvFileName}&compressed={compressed}";
        if (schemaInterpretation != null) {
            urlPattern += "&schemaInterpretation={schemaInterpretation}";
            args.add(schemaInterpretation);
        }
        if (!StringUtils.isEmpty(entity)) {
            urlPattern += "&entity={entity}";
            args.add(entity);
        }
        String url = constructUrl(urlPattern, args.toArray(new Object[args.size()]));
        MultiValueMap<String, Object> parts = new LinkedMultiValueMap<>();
        parts.add("file", fileResource);
        ResponseDocument resp = postMultiPart("upload file", url, parts, ResponseDocument.class);
        if (resp.isSuccess()) {
            return JsonUtils.deserialize(JsonUtils.serialize(resp.getResult()), SourceFile.class);
        } else {
            throw new RuntimeException("Failed to upload file: "+ StringUtils.join(resp.getErrors(), ","));
        }
    }

    public SourceFile uploadFile(String fileName, boolean compressed, String csvFileName, String entity,
                            Resource fileResource){

        List<Object> args = new ArrayList<>();
        args.add(fileName);
        args.add(csvFileName);
        args.add(compressed);
        args.add(entity);
        String urlPattern = "/?fileName={fileName}&displayName={csvFileName}&compressed={compressed}" +
                "&entity={entity}";
        String url = constructUrl(urlPattern, args.toArray(new Object[args.size()]));
        MultiValueMap<String, Object> parts = new LinkedMultiValueMap<>();
        parts.add("file", fileResource);
        ResponseDocument resp = postMultiPart("upload file", url, parts, ResponseDocument.class);
        if (resp.isSuccess()) {
            return JsonUtils.deserialize(JsonUtils.serialize(resp.getResult()), SourceFile.class);
        } else {
            throw new RuntimeException("Failed to upload file: "+ StringUtils.join(resp.getErrors(), ","));
        }
    }

    public FieldMappingDocument getFieldMappings(String sourceFileName, String entity) {
        String urlPattern = "/{sourceFileName}/fieldmappings?entity={entity}";
        String url = constructUrl(urlPattern, sourceFileName, entity);
        ResponseDocument resp = post("get field mappings", url, null, ResponseDocument.class);
        if (resp.isSuccess()) {
            return JsonUtils.deserialize(JsonUtils.serialize(resp.getResult()), FieldMappingDocument.class);
        } else {
            throw new RuntimeException("Failed to get filed mapping: " + StringUtils.join(resp.getErrors(), ","));
        }
    }

    public void saveFieldMappingDocument(String displayName, FieldMappingDocument fieldMappingDocument) {
        String urlPattern = "/fieldmappings?displayName={displayName}";
        String url = constructUrl(urlPattern, displayName);
        post("save field mapping", url, fieldMappingDocument, Void.class);
    }

}
