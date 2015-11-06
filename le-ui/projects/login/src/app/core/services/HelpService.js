angular.module('mainApp.core.services.HelpService', [
    'mainApp.appCommon.utilities.URLUtility'
])
.service('HelpService', function (URLUtility) {
    
    /*
    * Navigates to the Privacy Policy web page.
    */
    this.OpenPrivacyPolicy = function() {
        URLUtility.OpenNewWindow("./help/PrivacyPolicy.html");
    };
});