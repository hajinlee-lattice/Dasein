angular.module('mainApp.campaigns.modals.CreateCampaignModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.campaigns.services.CampaignService',
    'mainApp.core.utilities.NavUtility'
])
.service('CreateCampaignModal', function ($compile, $templateCache, $rootScope, $http, ResourceUtility, CampaignService) {
    var self = this;
    this.show = function () {
        $http.get('app/campaigns/views/CreateCampaignModal.html', { cache: $templateCache }).success(function (html) {

            var scope = $rootScope.$new();

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
.controller('CreateCampaignController', function ($scope, $rootScope, $state, ResourceUtility, NavUtility, CampaignService) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.createAccountCampaignClick = function($event){
        if ($event != null) {
            $event.preventDefault();
        }
    };

    $scope.createLeadCampaignClick = function($event){
        if ($event != null) {
            $event.preventDefault();
        }
    };


    $scope.deleteModelClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        updateAsDeletedModel($scope.modelId);
    };

    function updateAsDeletedModel(modelId) {
        $("#deleteModelError").hide();
        ModelService.updateAsDeletedModel(modelId).then(function(result) {
            if (result != null && result.success === true) {
                $("#modalContainer").modal('hide');
                $state.go('home.models', {}, { reload: true } );
            } else {
                $scope.deleteModelErrorMessage = result.ResultErrors;
                $("#deleteModelError").fadeIn();
            }
        });
    }

    $scope.cancelClick = function () {
        $("#modalContainer").modal('hide');
    };
});
