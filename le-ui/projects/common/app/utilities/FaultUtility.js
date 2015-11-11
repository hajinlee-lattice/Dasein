angular.module('mainApp.appCommon.utilities.FaultUtility', [
    'mainApp.appCommon.utilities.ConfigConstantUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.modals.SimpleModal'
])                                                                                                                                                                        
.service('FaultUtility', function (ResourceUtility, BrowserStorageUtility, SimpleModal) {
    
    this.HandleServiceResponseFailure = function (response, faultTitle, failSilently) {
        failSilently = typeof failSilently !== 'undefined' ? failSilently : false;
        //Session Timeout?
        if (this.HandleSessionTimeout(response)) return;
        //Authentication Issue?
        if (this.HandleAuthenticationIssue(response)) return;
        
        if (!failSilently) {
            //Special Error?
            if (this.HandleSpecialErrors(response)) return;
            
            //Generic Error
            var message = this.GenerateErrorMessage(response.Errors);
            this.ShowFaultAlert(message, faultTitle);
        }
    };
    
    //This is used to handle calls that get no response from the server (i.e. server is down)
    this.HandleNoResponseFailure = function (response, settings) {
        faultTitle = settings.faultTitle;
        failSilently = settings.failSilently;
        failSilently = typeof failSilently !== 'undefined' ? failSilently : false;
        if (!failSilently) {
            
            var message = this.GetWebServiceFaultDetails(response, settings);
            this.ShowFaultAlert(message, settings.faultTitle);
        }
    };
    
    /*
     * Checks for a session timeout error in the list of errors
     * 
     */
    this.HandleSessionTimeout = function (response) {
        var result = false;
        var timeoutString = ResourceUtility.getString('USER_SESSION_TIMEOUT');
        if (this.GetUserDisplayableErrors(response).indexOf(timeoutString) != -1) {  
            this.ShowFaultAlert(timeoutString, ResourceUtility.getString('USER_SESSION_TIMEOUT_TITLE'), this.ResetApp);
            result = true;
        }
        
        return result;
    };
    
    this.HandleAuthenticationIssue = function (response) {
        var result = false;
        var expiredString = ResourceUtility.getString('LOGIN_EXPIRED_AUTHENTICATION_CREDENTIALS');
        var invalidString = ResourceUtility.getString('LOGIN_INVALID_AUTHENTICATION_CREDENTIALS');
        if (this.GetUserDisplayableErrors(response).indexOf(expiredString) != -1) {
            this.ResetApp();
            result = true;
        } else if (this.GetUserDisplayableErrors(response).indexOf(invalidString) != -1) {
            this.ShowFaultAlert(invalidString, ResourceUtility.getString('INVALID_AUTHENTICATION_CREDENTIALS_TITLE'), this.ResetApp);
            result = true;
        }
        
        return result;
    };
    
    this.HandleSpecialErrors = function (response) {
        if (response != null && response.Errors != null && response.Errors.length > 0) {
            for (var x = 0; x<response.Errors.length;x++) {
                var error = response.Errors[x];
                if (error.CodeString == ConfigConstantUtility.ImpersonationWriteError) {
                    this.ShowFaultAlert(ResourceUtility.getString(error.CodeString));
                    return true;
                } else if (error.CodeString == ConfigConstantUtility.ValidationInProgressError) {
                    var detailedDescription = null;
                    for (var y = 0; y<error.DataList.length;y++) {
                        var errorDetail = error.DataList[y];
                        if (errorDetail == ResourceUtility.getString(ConfigConstantUtility.DetailedDescriptionDataKey)) {
                            detailedDescription = errorDetail;
                            //this should only break out of the $.each() loop and not out of HandleSpecialErrors
                            break;
                        }
                    }
                    if (detailedDescription == null) {
                        detailedDescription = ResourceUtility.getString(error.CodeString);
                    }
                    this.ShowFaultAlert(detailedDescription, error.Message);
                    return true;
                }
            }
        }
        return false;
    };
    
    this.GetUserDisplayableErrors = function (response) {
        var result = "";
        
        if (response != null && response.Errors != null && response.Errors.length > 0) {
            for (var x = 0; x<response.Errors.length;x++) {
                var errorValue = response.Errors[x];
                var errorMessage = errorValue.Message;
                var fullErrorMessage = errorMessage.replace(errorValue.CodeString, ResourceUtility.getString(errorValue.CodeString));
                if (errorMessage === fullErrorMessage) {
                    result += ResourceUtility.getString(errorValue.CodeString) + " " + fullErrorMessage + " ";
                } else {
                    result += fullErrorMessage + " ";
                }
            }
        }
        
        return result;
    };
    
    this.GetWebServiceFaultDetails = function (response, settings) {
        var result = "";
        //Handle special faults
        switch (response.statusText) {
            case "timeout":
                var timeoutString = ResourceUtility.getString('REQUEST_TIMEOUT_BEGIN');
                var timeoutInSeconds = settings.timeout / 1000;
                var unitString = settings.timeout != 1000 ? ResourceUtility.getString('REQUEST_TIMEOUT_SECONDS') : ResourceUtility.getString('REQUEST_TIMEOUT_SECOND');
                if (timeoutString !== 'REQUEST_TIMEOUT_BEGIN') {
                    result = timeoutString + " " + timeoutInSeconds + " " + unitString + ".";
                } else {
                    unitString = " second" + (settings.timeout != 1000 ? "s." : ".");
                    result = "Request timed out after " + settings.timeout + unitString;
                }
                break;
        }
        if (result === "") {
            var message = ResourceUtility.getString('SERVER_UNAVAILABLE') != 'SERVER_UNAVAILABLE' ? 
                ResourceUtility.getString('SERVER_UNAVAILABLE') : 'Server temporarily unavailable. Please retry later.';  
            result = message;
            if(response.status !== 0) {
                result += "<br /><br />Status Code: " + response.status;
                //Report error to New Relic
                var loc = settings.url.indexOf('?');
                NewRelicWorker.SendActivity("Error: " + response.status + " " + settings.url.substring(0, loc), 1);
            }
            if(response.statusText != null) {
                result += "<br /><br />Fault Detail: " + response.statusText;
            }
        }
        return result;
    };
    
    this.GenerateErrorMessage = function (errors) {
        var toReturn = "";
        if(!$.isEmptyObject(errors)) {
            for(var x = 0; x<errors.length;x++) {
                var parentError = errors[x];
                var numChildErrors = 0;
                if(!$.isEmptyObject(parentError.InnerErrors)) {
                    numChildErrors = parentError.InnerErrors.length;
                }
                
                if (numChildErrors === 0 || (numChildErrors > 0 && parentError.CodeString != "VALIDATION_ERROR")) {
                    toReturn += this.GenerateSingleErrorMessage(parentError, (numChildErrors > 0));
                }
                
                if (numChildErrors > 0)
                {
                    toReturn += this.GenerateErrorMessage(parentError.InnerErrors);
                }
            }
        }
        return toReturn;
    };
    
    this.GenerateSingleErrorMessage = function (error, isParent) {
        isParent = typeof isParent !== 'undefined' ? isParent : false;
        if (!isParent) {
            return "ERROR: " + ResourceUtility.getString(error.CodeString) +"<br /><br />" + error.Message + "<br /><br /><br />";
        }
        return "PARENT ERROR: " + ResourceUtility.getString(error.CodeString) +"<br /><br />" + error.Message + "<br /><br /><br />";
    };
    
    this.ShowFaultAlert = function (faultMessage, faultTitle, callback) {
        SimpleModal.show({
            message: faultMessage, 
            title: faultTitle, 
            okCallback: callback
        });
    };
    
    this.ResetApp = function () {
        BrowserStorageUtility.clear(false);
        ResourceUtility.clearResourceStrings();
        window.location.hash = "/Initial";
    };
    
     //This is used to handle failures returned from our own rest calls and handle them without throwing a pop up
    this.HandleFriendlyServiceResponseErrors = function (response) {
        if (response == null) {
            return;
        }
        //Session Timeout?
        if (this.HandleSessionTimeout(response)) return;
        //Authentication Issue?
        if (this.HandleAuthenticationIssue(response)) return;
        
        return this.GenerateErrorMessage(response.Errors);
    };
});