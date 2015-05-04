angular.module('mainApp.config.modals.EnterCredentialsModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.config.services.ConfigService'
])

.service('EnterCredentialsModal', function ($compile, $rootScope, $http, ResourceUtility) {
    var self = this;
    
    this.TopologyType = {
        Salesforce: "sfdc",
        Marketo: "marketo",
        Eloqua: "eloqua"
    };
    
    this.show = function (topologyType, previousCredentials, successCallback) {
        $http.get('./app/config/views/EnterCredentialsView.html').success(function (html) {
            
            var scope = $rootScope.$new();
            scope.topologyType = topologyType;
            scope.previousCredentials = previousCredentials;
            scope.successCallback = successCallback;
            
            var modalElement = $("#modalContainer");
            $compile(modalElement.html(html))(scope);
            
            var options = {
                backdrop: "static"
            };
            modalElement.modal(options);
            modalElement.modal('show');
            
            // Remove the created HTML from the DOM
            modalElement.on('hidden.bs.modal', function (evt) {
                modalElement.empty();
            });
        });
    };
})
.controller('EnterCredentialsController', function ($scope, ResourceUtility, StringUtility, EnterCredentialsModal, ConfigService) {
    $scope.ResourceUtility = ResourceUtility;
    
    var topology = $scope.topologyType;
    
    $scope.validateErrorMessage = null;
    $scope.validateInProgess = false;
    
    // Credential fields
    $scope.apiUserId = null;
    $scope.apiPassword = null;
    $scope.securityToken = null;
    $scope.connectionUrl = null;
    $scope.company = null;
    
    // Validation classes
    $scope.apiUserIdInputError = "";
    $scope.apiPasswordInputError = "";
    $scope.securityTokenInputError = "";
    $scope.connectionUrlInputError = "";
    $scope.companyInputError = "";
    
    $("#enterCredentialsValidateError").hide();
    
    // Set previously entered credentials if they exist
    if ($scope.previousCredentials != null) {
        $scope.apiUserId = $scope.previousCredentials.UserName;
        // Have to add this hack because of DataLoader
        if (topology == EnterCredentialsModal.TopologyType.Marketo) {
            $scope.securityToken = $scope.previousCredentials.Password;
        } else {
            $scope.securityToken = $scope.previousCredentials.SecurityToken;
        }
        $scope.connectionUrl = $scope.previousCredentials.ConnectionURL;
        $scope.company = $scope.previousCredentials.Company;
    }
    
    
    switch (topology) {
        case EnterCredentialsModal.TopologyType.Salesforce:
            $scope.topologyTypeImage = "logo_salesforce.png";
            $scope.apiUserIdLabel = ResourceUtility.getString('ENTER_CREDENTIALS_API_USER_NAME');
            $scope.apiPasswordLabel = ResourceUtility.getString('ENTER_CREDENTIALS_API_PASSWORD');
            $scope.securityTokenLabel = ResourceUtility.getString('ENTER_CREDENTIALS_API_SECURITY_TOKEN');
            $scope.connectionUrlLabel = ResourceUtility.getString('ENTER_CREDENTIALS_API_ORG_ID');
            $scope.showPassword = true;
            $scope.showConnectionUrl = true;
            $scope.showCompany = false;
            $scope.showSecurityToken = true;
            break;
        case EnterCredentialsModal.TopologyType.Marketo:
            $scope.topologyTypeImage = "logo_marketo.png";
            $scope.apiUserIdLabel = ResourceUtility.getString('ENTER_CREDENTIALS_API_USER_ID');
            $scope.securityTokenLabel = ResourceUtility.getString('ENTER_CREDENTIALS_API_ENCRYPTION_KEY');
            $scope.connectionUrlLabel = ResourceUtility.getString('ENTER_CREDENTIALS_API_URL');
            $scope.showPassword = false;
            $scope.showConnectionUrl = true;
            $scope.showCompany = false;
            $scope.showSecurityToken = true;
            break;
        case EnterCredentialsModal.TopologyType.Eloqua:
            $scope.topologyTypeImage = "logo_eloqua.png";
            $scope.apiUserIdLabel = ResourceUtility.getString('ENTER_CREDENTIALS_API_USER_NAME');
            $scope.apiPasswordLabel = ResourceUtility.getString('ENTER_CREDENTIALS_API_PASSWORD');
            $scope.companyLabel = ResourceUtility.getString('ENTER_CREDENTIALS_API_COMPANY');
            $scope.showPassword = true;
            $scope.showCompany = true;
            $scope.showConnectionUrl = false;
            $scope.showSecurityToken = false;
            break;
    }
    
    function areCredentialsValid () {
        var isValid = false;
        switch (topology) {
            case EnterCredentialsModal.TopologyType.Salesforce:
                isValid = !StringUtility.IsEmptyString($scope.apiUserId) && !StringUtility.IsEmptyString($scope.apiPassword) &&
                    !StringUtility.IsEmptyString($scope.securityToken) && !StringUtility.IsEmptyString($scope.connectionUrlLabel);
                break;
            case EnterCredentialsModal.TopologyType.Marketo:
                isValid = !StringUtility.IsEmptyString($scope.apiUserId) && !StringUtility.IsEmptyString($scope.securityToken) &&
                    !StringUtility.IsEmptyString($scope.connectionUrl);
                break;
            case EnterCredentialsModal.TopologyType.Eloqua:
                isValid = !StringUtility.IsEmptyString($scope.apiUserId) && !StringUtility.IsEmptyString($scope.apiPassword) &&
                    !StringUtility.IsEmptyString($scope.securityToken);
                break;
        }
        
        return isValid;
    }
    
    function updateDisplayValidation () {
        switch (topology) {
            case EnterCredentialsModal.TopologyType.Salesforce:
                $scope.apiUserIdInputError = ($scope.apiUserId == null || $scope.apiUserId === "") ? "error" : "";
                $scope.apiPasswordInputError = ($scope.apiPassword == null || $scope.apiPassword === "") ? "error" : "";
                $scope.securityTokenInputError = ($scope.securityToken == null || $scope.securityToken === "") ? "error" : "";
                $scope.connectionUrlInputError = ($scope.connectionUrl == null || $scope.connectionUrl === "") ? "error" : "";
                break;
            case EnterCredentialsModal.TopologyType.Marketo:
                $scope.apiUserIdInputError = ($scope.apiUserId == null || $scope.apiUserId === "") ? "error" : "";
                $scope.securityTokenInputError = ($scope.securityToken == null || $scope.securityToken === "") ? "error" : "";
                $scope.connectionUrlInputError = ($scope.connectionUrl == null || $scope.connectionUrl === "") ? "error" : "";
                break;
            case EnterCredentialsModal.TopologyType.Eloqua:
                $scope.apiUserIdInputError = ($scope.apiUserId == null || $scope.apiUserId === "") ? "error" : "";
                $scope.apiPasswordInputError = ($scope.apiPassword == null || $scope.apiPassword === "") ? "error" : "";
                $scope.companyInputError = ($scope.company == null || $scope.company === "") ? "error" : "";
                break;
        }
    }
    
    function createApiCredentialsObject () {
        var toReturn = {
            Type: $scope.topologyType
        };
        switch (topology) {
            case EnterCredentialsModal.TopologyType.Salesforce:
                toReturn.UserName = $scope.apiUserId;
                toReturn.Password = $scope.apiPassword;
                toReturn.SecurityToken = $scope.securityToken;
                toReturn.Url = $scope.connectionUrl;
                break;
            case EnterCredentialsModal.TopologyType.Marketo:
                toReturn.UserName = $scope.apiUserId;
                // Have to add this hack because of DataLoader
                toReturn.Password = $scope.securityToken;
                toReturn.Url = $scope.connectionUrl;
                break;
            case EnterCredentialsModal.TopologyType.Eloqua:
                toReturn.UserName = $scope.apiUserId;
                toReturn.Password = $scope.apiPassword;
                toReturn.Company = $scope.company;
                break;
        }
        
        return toReturn;
    }
    
    // Handling the Save/Validate actions
    var successCallback = $scope.successCallback;
    $scope.saveClicked = function () {
        $("#enterCredentialsValidateError").fadeOut();
        $scope.validateErrorMessage = null;
        updateDisplayValidation();
        if (areCredentialsValid() && !$scope.validateInProgess) {
            var apiObj = createApiCredentialsObject();
            $scope.validateInProgess = true;
            ConfigService.ValidateApiCredentials($scope.topologyType, apiObj).then(function(data) {
                $scope.validateInProgess = false;
                if (data != null && data.Success === true) {
                    $("#modalContainer").modal('hide');
                    if (successCallback != null && typeof successCallback === 'function') {
                        successCallback(apiObj);
                    }
                } else {
                    // Credentials are not valid
                    if (data.FailureReason == "VALIDATE_CREDENTIALS_FAILURE") {
                        $scope.validateErrorMessage = ResourceUtility.getString("VALIDATE_CREDENTIALS_FAILURE");
                    } else {
                        $scope.validateErrorMessage = ResourceUtility.getString('ENTER_CREDENTIALS_CONNECTION_ERROR');
                    } 
                    $("#enterCredentialsValidateError").fadeIn();
                }
            });
        }
    };
});
