angular.module('mainApp.campaigns.modals.DeleteCampaignModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'lp.campaigns'
])
.service('DeleteCampaignModal', function ($compile, $templateCache, $rootScope, $http, ResourceUtility, CampaignService) {
    var self = this;
    this.show = function (credentialId) {
        $http.get('app/marketo/views/DeleteCampaignModal.html', { cache: $templateCache }).success(function (html) {

            var scope = $rootScope.$new();
            scope.credentialId = credentialId;

            var modalElement = $("#modalContainer");
            $compile(modalElement.html(html))(scope);
            $("#deleteCampaignError").hide();

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
.controller('DeleteCampaignController', function ($scope, $rootScope, $state, ResourceUtility, CampaignService) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.deleteCampaignClick = function () {
        $("#deleteCampaignError").hide();
        CampaignService.DeleteCampaign($scope.campaignId).then(function(result) {
            if (result != null && result.success === true) {
                $("#modalContainer").modal('hide');
                $state.go('home.campaigns', {}, { reload: true } );
            } else {
                $scope.deleteCampaignErrorMessage = result.ResultErrors;
                $("#deleteCampaignError").fadeIn();
            }
        });
    };

    $scope.cancelClick = function () {
        $("#modalContainer").modal('hide');
    };
});
