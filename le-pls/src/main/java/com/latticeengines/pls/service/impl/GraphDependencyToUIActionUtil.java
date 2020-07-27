package com.latticeengines.pls.service.impl;

import static j2html.TagCreator.b;
import static j2html.TagCreator.div;
import static j2html.TagCreator.each;
import static j2html.TagCreator.li;
import static j2html.TagCreator.p;
import static j2html.TagCreator.ul;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.auth.GlobalTeam;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.Status;
import com.latticeengines.domain.exposed.exception.UIAction;
import com.latticeengines.domain.exposed.exception.UIActionException;
import com.latticeengines.domain.exposed.exception.UIActionUtils;
import com.latticeengines.domain.exposed.exception.View;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;

@Component
public class GraphDependencyToUIActionUtil {

    private static final String TITLE_DEFAULT_UPDATE_FAILED = "Update failed as system detected potential circular dependency";
    private static final String TITLE_MODELS_EXCEED_LIMIT = "Limit Reached";
    private static final String SEGMENT = "segment";
    private static final String MODEL ="model";

    public UIAction processUpdateSegmentResponse(MetadataSegment segment, Map<String, List<String>> dependencies) {
        UIAction uiAction;
        if (MapUtils.isNotEmpty(dependencies)) {
            AtomicInteger count = new AtomicInteger(0);
            dependencies.keySet().stream() //
                    .filter(k -> CollectionUtils.isNotEmpty(dependencies.get(k))) //
                    .forEach(k -> count.set(count.get() + dependencies.get(k).size()));
            uiAction = generateUIAction(String.format("Segment %s In Use", segment.getDisplayName()), View.Banner,
                    Status.Warning,
                    generateHtmlMsg(dependencies,
                            "Changing a segment that is in use may affect the scoring and rating configuration.",
                            String.format("This segment has %d dependencies", count.get())) + (segment.isViewOnly()
                            ? generateHtmlMsgWithTeamInfo(SEGMENT, segment.getTeam()) : ""));
        } else {
            if (segment.isViewOnly()) {
                uiAction = generateUIAction(String.format("Segment %s is not safe to edit", segment.getDisplayName()),
                        View.Banner, Status.Warning, generateHtmlMsgWithTeamInfo(SEGMENT, segment.getTeam()));
            } else {
                uiAction = generateUIAction(String.format("Segment %s is safe to edit", segment.getDisplayName()),
                        View.Notice, Status.Success, null);
            }
        }
        return uiAction;
    }

    public UIAction generateModelDependenciesAction(Map<String, List<String>> dependencies, RatingEngineSummary ratingEngineSummary) {
        UIAction uiAction;
        if (MapUtils.isNotEmpty(dependencies)) {
            String message = generateHtmlMsg(dependencies, "This model is in use.", null);
            uiAction = generateUIAction("Model In Use", View.Banner, Status.Warning,
                    message + (ratingEngineSummary.isViewOnly() ? generateHtmlMsgWithTeamInfo(MODEL, ratingEngineSummary.getTeam()) : ""));
        } else {
            if (ratingEngineSummary.isViewOnly()) {
                uiAction = generateUIAction("Model is not safe to edit", View.Banner,
                        Status.Warning, generateHtmlMsgWithTeamInfo(MODEL, ratingEngineSummary.getTeam()));
            } else {
                uiAction = generateUIAction("Model is safe to edit", View.Notice,
                        Status.Success, null);
            }
        }
        return uiAction;
    }

    private UIAction generateUIAction(String title, View view, Status status, String message) {
        return UIActionUtils.generateUIAction(title, view, status, message);
    }

    private String generateHtmlMsgWithTeamInfo(String entity, GlobalTeam authTeam) {
        StringBuffer html = new StringBuffer();
        html.append(div(String.format("This %s belongs to \"%s\", you are not allowed to edit it. You can ask admin to add you to the team.",
                entity, authTeam.getTeamName())).render());
        return html.toString();
    }

    public String generateHtmlMsg(Map<String, List<String>> dependencies, String messageHeader, String depListHeader) {
        StringBuilder html = new StringBuilder();
        html.append(div(messageHeader).render());
        if (StringUtils.isNotBlank(depListHeader)) {
            html.append(p(depListHeader).render());
        }
        dependencies.forEach((k, v) -> {
            html.append((b(k + "(s): ").render()));
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
        return handleExceptionForCreateOrUpdate(ex, codeToHandle, View.Banner);
    }

    public UIActionException handleExceptionForCreateOrUpdate(Exception ex, LedpCode codeToHandle, View view) {
        if(codeToHandle.equals(LedpCode.LEDP_40074)) {
            return handleExceptionForCreateOrUpdate(ex, codeToHandle, view,
                    TITLE_MODELS_EXCEED_LIMIT, null);
        } else {
            return handleExceptionForCreateOrUpdate(ex, codeToHandle, view,
                    TITLE_DEFAULT_UPDATE_FAILED, null);
        }
    }

    public UIActionException handleExceptionForCreateOrUpdate(Exception ex, LedpCode codeToHandle, View view,
            String title, String messageHeader) {
        UIAction uiAction = null;
        LedpCode code = LedpCode.LEDP_00002;
        Status status = Status.Error;
        String message = ex.getMessage();
        if (ex instanceof LedpException) {
            code = ((LedpException) ex).getCode();
            if (codeToHandle.equals(code)) {
                String removeStartingText = codeToHandle.name() + ": ";
                if (message.startsWith(removeStartingText)) {
                    message = message.substring(removeStartingText.length());
                }
                if (LedpCode.LEDP_40042.equals(code)) {
                    uiAction = handleDeleteFailedDueToDependency((LedpException) ex, code, title, messageHeader, view,
                            null, null);
                }
            }
        } else {
            title = "Action failed";
        }
        if (uiAction == null) {
            uiAction = generateUIAction(title, view, status, message);
        }
        return new UIActionException(uiAction, code);
    }

    public UIAction handleDeleteFailedDueToDependency(LedpException ex, LedpCode codeToProcess,
            String titleForSpecificCode, String messageHeaderForSpecificCode, View viewForSpecificCode,
            String defaultTitle, View defaultView) {
        UIAction uiAction;
        if (ex != null && ex.getCode() == codeToProcess) {
            uiAction = generateUIAction(titleForSpecificCode, viewForSpecificCode, Status.Error, generateHtmlMsg(
                    extractDependencies(ex.getMessage(), LedpCode.LEDP_40042), messageHeaderForSpecificCode, null));
        } else if (ex != null) {
            uiAction = generateUIAction(defaultTitle, defaultView, Status.Error, ex.getMessage());
        } else {
            uiAction = generateUIAction(defaultTitle, defaultView, Status.Error, "Unknown error.");
        }
        return uiAction;
    }

    public UIActionException handleInvalidBucketsError(LedpException ex, String title) {
        String[] attrs = ex.getMessage().replace("Detected invalid buckets: ", "").split(",");
        String message;
        if (attrs.length > 1) {
            // TODO: Ideally, we should convert backend attr names to display names. For now, just remove them.
            message = "Detected " + attrs.length + " invalid buckets.";
        } else {
            message = "Detected invalid bucket.";
        }
        UIAction uiAction = generateUIAction(title, View.Banner, Status.Error, message);
        return new UIActionException(uiAction, LedpCode.LEDP_40057);
    }
}
