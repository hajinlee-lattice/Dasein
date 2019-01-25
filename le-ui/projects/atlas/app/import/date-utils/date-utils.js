const getDateFormat = (field) => {
    let date = field.dateFormatString;
    if (date == null) {
        return '';
    }
    return date;
};

const setDateFormat = (field, newFormat) => {
    field.dateFormatString = newFormat;
};

const getTimeFormat = (field) => {
    let time = field.timeFormatString;
    if (time == null ) {
        return '';
    }
    return time;
};

const getTimezone = (field) => {
    if (field.timezone != null) {
        return field.timezone;
    }
    return '';
};

const setTimeFormat = (field, newTimeFormat) => {
    let ret = newTimeFormat;
    field.timeFormatString = ret.trim();
};
const setTimezone = (field, newTimeZone) => {
    field.timezone = newTimeZone;
}

const isOnlyDateMandatory = (fieldMappingsOriginal, field) => {
    let userFieldName = field.userField;
    let fieldType = field.fieldType;
    let originalMapping = fieldMappingsOriginal.map[userFieldName];
    if (originalMapping.fieldType == fieldType && fieldType == 'DATE') {
        return isOnlyDate(originalMapping);
    }
    return true;
}

const isTimezoneMandatory = (fieldMappingOriginal, fieldMappingChanged) => {
    if (isDateField(fieldMappingOriginal) && !isOnlyDate(fieldMappingChanged)) {
        return true;
    }
    return false;
}


const isOnlyDate = (fieldMappingOriginal) => {
    let dateTime = fieldMappingOriginal.dateTimeFormatString;
    if (dateTime == null || dateTime.indexOf(' ') < 0) {
        return true;
    }
    return false;
}
const isDateField = (fieldMapping) => {
    if (fieldMapping && fieldMapping.fieldType == "DATE") {
        return true;
    }
    return false;
}
export {
    isOnlyDateMandatory, isTimezoneMandatory, getDateFormat, setDateFormat, getTimeFormat, getTimezone,
    setTimeFormat, setTimezone
};