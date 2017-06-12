angular.module('mainApp.playbook.content.playList.deletePlayModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.utilities.NavUtility'
])
.service('DeletePlayModal', function ($compile, $templateCache, $rootScope, $http, ResourceUtility, PlaybookWizardService) {
    var self = this;
    this.show = function (play) {
        $http.get('app/playbook/content/playList/playList.deleteModal.component.html', { cache: $templateCache }).success(function (html) {

            var scope = $rootScope.$new();
            scope.playName = play.name;

            var modalElement = $("#modalContainer");
            $compile(modalElement.html(html))(scope);
            $("#deleteModelError").hide();

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
.controller('DeletePlayController', function ($scope, $rootScope, $timeout, $state, ResourceUtility, NavUtility, PlaybookWizardService) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.deletePlayClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();

        }
        deletePlay($scope.playName);
    };

    function deletePlay(playName) {
        $("#deletePlayError").hide();

        PlaybookWizardService.deletePlay(playName).then(function(result) {
            
            $timeout( function(){
                $("#modalContainer").modal('hide');
                $state.go('home.playbook.plays', {}, { reload: true } );
            }, 100 );

        });
    }

    $scope.cancelClick = function () {
        $("#modalContainer").modal('hide');
    };

});
