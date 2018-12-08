package com.latticeengines.ulysses.utils;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.TalkingPointDTO;

@Component(TalkingPointDanteFormatter.Qualifier)
public class TalkingPointDanteFormatter implements DanteFormatter<TalkingPointDTO> {

    public static final String Qualifier = "talkingPointDanteFormatter";

    private static final String notionName = "DanteTalkingPoint";

    private class DanteTalkingPoint {
        private TalkingPointDTO talkingPointDTO;

        private DanteTalkingPoint(TalkingPointDTO talkingPointDTO) {
            this.talkingPointDTO = talkingPointDTO;
        }

        @JsonProperty(value = "BaseExternalID", index = 1)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getBaseExternalId() {
            return talkingPointDTO.getName();
        }

        @JsonProperty(value = "NotionName", index = 2)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getNotionName() {
            return notionName;
        }

        @JsonProperty(value = "PlayID", index = 6)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getPlayId() {
            return talkingPointDTO.getPlayName();
        }

        @JsonProperty(value = "Title", index = 8)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getTitle() {
            return talkingPointDTO.getTitle();
        }

        @JsonProperty(value = "Content", index = 3)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getContent() {
            return talkingPointDTO.getContent();
        }

        @JsonProperty(value = "Offset", index = 5)
        @JsonView(DanteFormatter.DanteFormat.class)
        public int getOffset() {
            return talkingPointDTO.getOffset();
        }

        @JsonProperty(value = "LastModified", index = 4)
        @JsonView(DanteFormatter.DanteFormat.class)
        public Date getLastModified() {
            return talkingPointDTO.getUpdated();
        }

        @JsonProperty(value = "TalkingPointID", index = 7)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getTalkingPointID() {
            return talkingPointDTO.getName();
        }

        @Override
        public String toString() {
            return JsonUtils.serialize(this, DanteFormatter.DanteFormat.class);
        }
    }

    @Override
    public String format(TalkingPointDTO entity) {
        return new DanteTalkingPoint(entity).toString();
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<String> format(List<TalkingPointDTO> entities) {
        return entities != null //
                ? entities.stream().map(this::format).collect(Collectors.toList()) //
                : Collections.EMPTY_LIST;
    }
}
