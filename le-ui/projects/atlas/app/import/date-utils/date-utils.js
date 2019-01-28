export default class DateUtils {
    static getDateFormat(field) {
        let date = field.dateFormatString;
        if (date == null) {
            return '';
        }
        return date;
    }
    static setDateFormat(field, newFormat) {
        field.dateFormatString = newFormat;
    }

    static getTimeFormat(field) {
        let time = field.timeFormatString;
        if (time == null) {
            return '';
        }
        return time;
    }

    static getTimezone(field) {
        if (field.timezone != null) {
            return field.timezone;
        }
        return '';
    }
    static setTimeFormat(field, newTimeFormat) {
        let ret = newTimeFormat;
        field.timeFormatString = ret.trim();
    }
    static setTimezone(field, newTimeZone) {
        field.timezone = newTimeZone;
    }

    static isOnlyDateMandatory(fieldMappingsOriginal, field) {
        let userFieldName = field.userField;
        let fieldType = field.fieldType;
        let originalMapping = fieldMappingsOriginal.map[userFieldName];
        if (originalMapping.fieldType == fieldType && fieldType == 'DATE') {
            return isOnlyDate(originalMapping);
        }
        return true;
    }
}