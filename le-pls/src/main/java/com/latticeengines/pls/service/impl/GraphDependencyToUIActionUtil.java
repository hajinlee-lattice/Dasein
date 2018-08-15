package com.latticeengines.pls.service.impl;

import static j2html.TagCreator.b;
import static j2html.TagCreator.each;
import static j2html.TagCreator.li;
import static j2html.TagCreator.ul;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.UIActionException;
import com.latticeengines.domain.exposed.pls.frontend.Status;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.pls.frontend.View;

@Component
public class GraphDependencyToUIActionUtil {

    public UIAction processUpdateResponse(Map<String, List<String>> dependencies) {
        UIAction uiAction;
        if (MapUtils.isNotEmpty(dependencies)) {
            uiAction = generateUIAction("Following entites may get impacted if segment is updated", View.Modal,
                    Status.Warning, generateHtmlMsg(dependencies));
        } else {
            uiAction = generateUIAction("Segment is safe to edit", View.Notice, Status.Success, null);
        }
        return uiAction;
    }

    public UIAction generateUIAction(String title, View view, Status status, String message) {
        UIAction uiAction;
        uiAction = new UIAction();
        uiAction.setTitle(title);
        uiAction.setView(view);
        uiAction.setStatus(status);
        uiAction.setMessage(message);
        return uiAction;
    }

    public String generateHtmlMsg(Map<String, List<String>> dependencies) {
        StringBuilder html = new StringBuilder();
        dependencies.forEach((k, v) -> {
            html.append((b(k + ": ").render()));
            html.append(ul().with( //
                    each(v, attr -> //
            li(attr))).render());
        });
        return html.toString();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Map<String, List<String>> extractDependencies(String message, LedpCode ledpCode) {
        Map<String, List<String>> translatedDependencies = new HashMap<>();
        String endPartOfStatisErrMgs = ": ";
        String staticPartOfErrorMsg = ledpCode.getMessage() //
                .substring(0, ledpCode.getMessage() //
                        .lastIndexOf(endPartOfStatisErrMgs) + endPartOfStatisErrMgs.length());
        String objectStr = message.substring(message.indexOf(staticPartOfErrorMsg))
                .substring(staticPartOfErrorMsg.length()).replace("\\\"", "\"");
        Map intermediateTranslatedDependencies = JsonUtils.deserialize(objectStr, Map.class);
        translatedDependencies.putAll(
                JsonUtils.convertMapWithListValue(intermediateTranslatedDependencies, String.class, String.class));
        return translatedDependencies;
    }

    public UIActionException handleExceptionForCreateOrUpdate(Exception ex, LedpCode codeToHandle) {
        UIAction uiAction = new UIAction();
        LedpCode code = LedpCode.LEDP_00002;
        String title = "Action failed";
        View view = View.Banner;
        Status status = Status.Error;
        String message = ex.getMessage();
        if (ex instanceof LedpException) {
            code = ((LedpException) ex).getCode();
            if (code == codeToHandle) {
                message = ex.getMessage();
                String removeStartingText = codeToHandle.name() + ": ";
                if (message.startsWith(removeStartingText)) {
                    message = message.substring(removeStartingText.length());
                }
                title = "Update failed as system detected potential circular dependency";
            }
        }
        uiAction = generateUIAction(title, view, status, message);
        return new UIActionException(uiAction, code);
    }

}
