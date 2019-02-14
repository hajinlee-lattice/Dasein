export default class DateUtils {
    static getDateFormat(field, listSupported) {
        let date = field.dateFormatString;
        if (date == null || !DateUtils.isFormatSupporte(listSupported, field.dateFormatString)) {
            return '';
        }
        return date;
    }
    static setDateFormat(field, newFormat) {
        field.dateFormatString = newFormat;
    }

    static getTimeFormat(field, listSupported) {
        let time = field.timeFormatString;
        if (time == null || !DateUtils.isFormatSupporte(listSupported, field.timeFormatString)) {
            return '';
        }
        return time;
    }
    static isFormatSupporte(listSupported, format){
        let supported = false;
        if(listSupported){
            listSupported.forEach(element => {
                if(element == format){
                    supported = true;
                    return;
                }
            });
        }
        // console.log('FORMAT SUPORTED ==> ', format, listSupported , supported);
        return supported;
    }
    static getTimezone(field, listSupported) {
        // console.log('UTIL TZ => ', field);
        if (field.timezone != null && DateUtils.isFormatSupporte(listSupported, field.timezone)) {
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