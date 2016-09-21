angular.module('lp.marketo.setup', [
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('ModelsSetupController', ['$scope', '$state', '$stateParams', 'BrowserStorageUtility', 'ResourceUtility', 'MarketoCredentials', function($scope, $state, $stateParams, BrowserStorageUtility, ResourceUtility, MarketoCredentials){
    
    $scope.credentialIsSetup = false;
    $scope.credentials = MarketoCredentials.resultObj;

	$scope.createCredentialClick = function($event){
		
	}

	// $scope.editCredentialClick = function($event){
		
	// }

	// $scope.setupCredentialClick = function($event){
		
	// }

	// $scope.deleteCredentialClick = function($event){

	// }
	
}]);