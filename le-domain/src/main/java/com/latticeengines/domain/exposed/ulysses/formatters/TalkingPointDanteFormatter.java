package com.latticeengines.domain.exposed.ulysses.formatters;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.TalkingPointDTO;

@Component(TalkingPointDanteFormatter.Qualifier)
public class TalkingPointDanteFormatter implements DanteFormatter<TalkingPointDTO> {

    public static final String Qualifier = "talkingPointDanteFormatter";

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
