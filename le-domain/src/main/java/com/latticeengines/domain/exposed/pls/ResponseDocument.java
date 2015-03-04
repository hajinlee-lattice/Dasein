package com.latticeengines.domain.exposed.pls;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;

public class ResponseDocument<ResultType> {

    private boolean success;
    private List<String> errors;
    private ResultType result;

    public ResponseDocument() { }

    @JsonProperty("Errors")
    public List<String> getErrors() { return errors; }

    @JsonProperty("Errors")
    public void setErrors(List<String> errors) { this.errors = errors; }

    @JsonProperty("Result")
    public ResultType getResult() { return result; }

    @JsonProperty("Result")
    public void setResult(ResultType result) {
        this.result = result;
    }

    @JsonProperty("Success")
    public boolean isSuccess() {
        return success;
    }

    @JsonProperty("Success")
    public void setSuccess(boolean success) {
        this.success = success;
    }

    @Override
    public String toString() { return JsonUtils.serialize(this); }

    public static ResponseDocument generateFromJSON(String json, Class resultType) throws IOException {
        ResponseDocument deserializedDoc = new ResponseDocument<>();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(json);

        deserializedDoc.setSuccess(node.get("Success").asBoolean());

        if (node.get("Errors") != null && node.get("Errors").size() > 0) {
            List<String> errors = new ArrayList<>();
            for (JsonNode errorNode: node.get("Errors")){
                errors.add(errorNode.asText());
            }
            deserializedDoc.setErrors(errors);
        }

        deserializedDoc.setResult(mapper.treeToValue(node.get("Result"), resultType));

        return deserializedDoc;
    }
}
