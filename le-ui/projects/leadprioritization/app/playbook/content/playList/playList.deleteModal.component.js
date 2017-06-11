angular.module('lp.playbook.DeletePlayModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.utilities.NavUtility'
])
.service('  ', function ($compile, $templateCache, $rootScope, $http, ResourceUtility, PlayListService) {
    var self = this;
    this.show = function (play) {
        $http.get('app/playbook/content/playList/DeletePlayConfirmView.html', { cache: $templateCache }).success(function (html) {

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
.controller('DeletePlayController', function ($scope, $rootScope, $state, ResourceUtility, NavUtility, PlayListService) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.deletePlayClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        deletePlay($scope.playName);
    };

    function deletePLay(PlayName) {
        $("#deletePlayError").hide();

        PlayListService.DeletePlay(playName).then(function(result) {
            if (result != null && result.success === true) {
                $("#modalContainer").modal('hide');
                if ($scope.inModel) {
                    $state.go('home.playbook', {}, { reload: true } );
                } else {
                    $state.go('home.playbook', {}, { reload: true } );
                }
            } else {
                $scope.deletePlayErrorMessage = result.ResultErrors;
                $("#deletePlayError").fadeIn();
            }
        });
    }

    $scope.cancelClick = function () {
        $("#modalContainer").modal('hide');
    };

});
