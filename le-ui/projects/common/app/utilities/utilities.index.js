console.log('Utilities module');
// import angular from "angular";
import * as jStorage from 'jstorage';

import './AuthorizationUtility';
import './BrowserStorageUtility';
import './DateTimeFormatUtility';
import './NavUtility';
import './NumberUtility';
import './ResourceUtility';
import './PasswordUtility';
import './UnderscoreUtility';
import './BrowserStorageUtility';
import './RightsUtility';
import './SegmentsUtility';
import './StringUtility';
import './TimestampIntervalUtility';
import './URLUtility';

export default angular.module('com.le.common.utilities', [
    'mainApp.core.utilities.AuthorizationUtility',
    'common.utilities.browserstorage',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.core.utilities.NavUtility',
    'common.utilities.number',
    'mainApp.core.utilities.PasswordUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.appCommon.utilities.SegmentsUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.appCommon.utilities.TimestampIntervalUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.appCommon.utilities.URLUtility'
]);
