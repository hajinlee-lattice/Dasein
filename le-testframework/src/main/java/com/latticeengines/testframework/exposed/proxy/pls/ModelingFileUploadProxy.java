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
import com.latticeengines.domain.exposed.pls.EntityExternalType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;

@Service("modelingFileUploadProxy")
public class ModelingFileUploadProxy extends PlsRestApiProxyBase {

    public ModelingFileUploadProxy() {
        super("pls/models/uploadfile");
    }

    @SuppressWarnings("unchecked")
    public SourceFile uploadFile(String fileName, boolean compressed, String csvFileName, SchemaInterpretation schemaInterpretation, //
                           EntityExternalType entityExternalType, Resource fileResource){
        List<Object> args = new ArrayList<>();
        args.add(fileName);
        args.add(csvFileName);
        args.add(compressed);
        String urlPattern = "/?fileName={fileName}&displayName={csvFileName}&compressed={compressed}";
        if (schemaInterpretation != null) {
            urlPattern += "&schemaInterpretation={schemaInterpretation}";
            args.add(schemaInterpretation);
        }
        if (entityExternalType != null) {
            urlPattern += "$entityExternalType={entityExternalType}";
            args.add(entityExternalType);
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

}
