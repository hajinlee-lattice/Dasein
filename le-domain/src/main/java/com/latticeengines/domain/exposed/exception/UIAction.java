package com.latticeengines.domain.exposed.exception;

import java.util.Collections;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class UIAction extends UIMessage {

    @JsonProperty("code")
    private UIActionCode code;

    @JsonProperty("title")
    private String title;

    @JsonProperty("view")
    private View view;

    public UIAction() {
    }

    public UIAction(UIActionCode code) {
        this(code, Collections.emptyMap());
    }

    public UIAction(UIActionCode code, Map<String, Object> params) {
        if (MapUtils.isNotEmpty(params)) {
            setMessage(code.renderMessage(params));
        } else {
            setMessage(code.renderMessage(Collections.emptyMap()));
        }
        this.code = code;
    }

    public UIAction(UIActionCode code, UIActionParams params) {
        if (params != null) {
            setMessage(code.renderMessage(params));
        } else {
            setMessage(code.renderMessage(Collections.emptyMap()));
        }
        this.code = code;
    }

    public String getTitle() {
        return this.title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public View getView() {
        return this.view;
    }

    public void setView(View view) {
        this.view = view;
    }

    public UIActionCode getCode() {
        return code;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
