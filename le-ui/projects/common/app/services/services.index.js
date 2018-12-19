console.log('Services module');
// import angular from "angular";s
import './ConfigService';
import './SessionService';
import './FeatureFlagService';
import './HelpService';
import './LoginService';
import './MetadataService';
import './ResourceStringsService';

export default angular.module('com.le.common.services', [
    'mainApp.config.services.ConfigService',
    'mainApp.core.services.FeatureFlagService',
    'mainApp.core.services.HelpService',
    'mainApp.login.services.LoginService',
    'mainApp.setup.services.MetadataService',
    'mainApp.core.services.ResourceStringsService',
    'mainApp.core.services.SessionService'
]);
