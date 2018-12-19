angular.module('mainApp.segments.modals.DeleteSegmentModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.utilities.NavUtility'
])
.service('DeleteSegmentModal', function ($compile, $templateCache, $rootScope, $http, ResourceUtility, SegmentService) {
    var self = this;
    this.show = function (segment, inModel) {
        $http.get('app/segments/modals/DeleteSegmentConfirmView.html', { cache: $templateCache }).success(function (html) {

            var scope = $rootScope.$new();
            scope.segmentName = segment.name;
            scope.displayName = segment.displayName;
            scope.inModel = inModel;

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
.controller('DeleteSegmentController', function ($scope, $stateParams, $rootScope, $state, ResourceUtility, NavUtility, SegmentService) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.deleteSegmentClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        deleteSegment($scope.segmentName);
    };

    function deleteSegment(segmentName) {

        $stateParams.edit = null;
        $("#deleteSegmentError").hide();

        SegmentService.DeleteSegment(segmentName).then(function(result) {
            if (result != null && result.success === true) {
                $("#modalContainer").modal('hide');
                if ($scope.inModel) {
                    $state.go('home.model.segmentation', {}, { reload: true } );
                } else {
                    $state.go('home.segments', {}, { reload: true } );
                }
            } else {
                $scope.deleteSegmentErrorMessage = result.errorMessage;
                $("#modalContainer").modal('hide');
            }
        });
    }

    $scope.cancelClick = function () {
        $("#modalContainer").modal('hide');
    };

});
