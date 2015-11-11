angular.module('mainApp.core.utilities.ServiceErrorUtility', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.FaultUtility',
    'mainApp.appCommon.modals.SimpleModal'
])
.service('ServiceErrorUtility', function (FaultUtility, ResourceUtility, SimpleModal) {
    
    this.InvalidCredentials = "LOGIN_INVALID_AUTHENTICATION_CREDENTIALS";
    this.ExpiredCredentials = "LOGIN_EXPIRED_AUTHENTICATION_CREDENTIALS";
    this.ExpiredPassword = "LOGIN_EXPIRED_USER_PASSWORD";
    /*
     * This is used to handle failures returned from our own rest calls and handle them without throwing a pop up
     */
    this.HandleFriendlyServiceResponseErrors = function (response) {
        if (response == null) {
            return;
        }
        // Session Timeout?
        if (this.HandleSessionTimeout(response)) {
            return;
        }
        
        return this.GetUserDisplayableErrors(response);
    };
    
    this.HandleServiceResponseFailure = function (response, faultTitle, failSilently) {
        failSilently = typeof failSilently !== 'undefined' ? failSilently : false;
        
        if (!failSilently) {
            // Generic Error
            var message = this.HandleFriendlyServiceResponseErrors(response);
            FaultUtility.ShowFaultAlert(message, faultTitle);
        }
    };
    
    // Need to handle the scenario where a service is not responding at all
    this.HandleFriendlyNoResponseFailure = function () {
        // Need to show a better error message if it fails to get the resource strings
        var resultError = ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR');
        if (resultError == "UNEXPECTED_SERVICE_ERROR") {
            resultError = "Unknown error occurred.";
        }
        var result = {
            success: false,
            resultObj: null,
            resultErrors: resultError
        };
        return result;
    };
    
    /*
     * Checks for a session timeout error in the list of errors
     */
    this.HandleSessionTimeout = function (response) {
        var result = false;
        if (this.ServiceResponseContainsError(response, this.ExpiredCredentials)) {
            SimpleModal.show({
                message: ResourceUtility.getString(this.ExpiredCredentials), 
                title: ResourceUtility.getString('EXPIRED_SESSION_TITLE'),
                showCloseButton: false
            });
            result = true;
        }
        
        return result;
    };
    
    this.GetUserDisplayableErrors = function (response) {
        var result = "";
        
        if (response != null && response.Errors != null && response.Errors.length > 0) {
            for (var x = 0; x < response.Errors.length; x++) {
                var errorValue = response.Errors[x].Key;
                var errorMessage = response.Errors[x].Value;
                var fullErrorMessage = errorMessage.replace(errorValue, ResourceUtility.getString(errorValue));
                result += fullErrorMessage;
            }
        }
        
        return result;
    };
    
    // Checks the Service Response for a specific error code
    this.ServiceResponseContainsError = function (response, errorCode) {
        var result = false;
        if (response != null && response.Errors != null && response.Errors.length > 0) {
            for (var x = 0; x < response.Errors.length; x++) {
                if (response.Errors[x].Key == errorCode) {
                    result = true;
                }
            }
        }
        return result;
    };
    
    this.ServiceResultError = "";
    
    this.ShowErrorView = function (response) {
        if (response == null) {
            this.ServiceResultError = this.HandleFriendlyNoResponseFailure();
        } else {
            this.ServiceResultError = response.resultErrors;
        }
        window.location.hash = '/ServiceError';
    };
});