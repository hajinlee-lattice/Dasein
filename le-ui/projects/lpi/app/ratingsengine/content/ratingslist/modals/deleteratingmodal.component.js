angular.module('mainApp.ratingsengine.deleteratingmodal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility'
])
.service('DeleteRatingModal', function ($compile, $templateCache, $rootScope, $http) {
    var self = this;
    this.show = function (rating) {
        $http.get('app/ratingsengine/content/ratingslist/modals/deleteratingmodal.component.html', { cache: $templateCache }).success(function (html) {
            var scope = $rootScope.$new();
            scope.ratingId = rating.id;

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
.controller('DeleteRatingController', function ($scope, $timeout, ResourceUtility, RatingsEngineStore) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.deleteRatingClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        deleteRating($scope.ratingId);
    };

    function deleteRating(ratingId) {
        $("#deleteRatingError").hide();
        $("#modalContainer").modal('hide');
        
        RatingsEngineStore.deleteRating(ratingId);//.then(function(result) {});
    }

    $scope.cancelClick = function () {
        $("#modalContainer").modal('hide');
    };

});
