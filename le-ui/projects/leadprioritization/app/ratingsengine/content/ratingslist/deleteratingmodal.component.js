angular.module('mainApp.ratingsengine.deleteratingmodal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.utilities.NavUtility'
])
.service('DeleteRatingModal', function ($compile, $templateCache, $rootScope, $http, ResourceUtility, RatingsEngineService) {
    var self = this;
    this.show = function (rating) {
        $http.get('app/ratingsengine/content/ratingslist/deleteratingmodal.component.html', { cache: $templateCache }).success(function (html) {

            var scope = $rootScope.$new();
            scope.ratingName = rating.name;

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
.controller('DeleteRatingController', function ($scope, $rootScope, $timeout, $state, ResourceUtility, NavUtility, RatingsEngineService) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.deleteRatingClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();

        }
        deleteRating($scope.ratingName);
    };

    function deleteRating(ratingName) {
        $("#deleteRatingError").hide();

        RatingsEngineService.deleteRating(ratingName).then(function(result) {
            
            $timeout( function(){
                $("#modalContainer").modal('hide');
                $state.go('home.ratingsengine.ratings', {}, { reload: true } );
            }, 100 );

        });
    }

    $scope.cancelClick = function () {
        $("#modalContainer").modal('hide');
    };

});
