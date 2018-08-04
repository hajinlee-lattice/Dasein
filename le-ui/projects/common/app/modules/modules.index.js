console.log("Modules module");
// import angular from "angular";

import './Pagination/Pagination';

import '../utilities/ResourceUtility';
import './ServiceError/ServiceErrorModule';

import '../utilities/BrowserStorageUtility';
import '../services/LoginService';
import './SessionTimeout/SessionTimeoutUtility';

export default angular.module("com.le.common.modules", [
    'pd.navigation.pagination',
    'mainApp.core.modules.ServiceErrorModule',
    'common.utilities.SessionTimeout'
    
]);
