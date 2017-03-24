angular.module('mainApp.models.modals.DeleteSegmentModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.utilities.NavUtility'
])
.service('DeleteSegmentModal', function ($compile, $templateCache, $rootScope, $http, ResourceUtility, SegmentService) {
    var self = this;
    this.show = function (modelId) {
        $http.get('app/models/views/DeleteSegmentConfirmView.html', { cache: $templateCache }).success(function (html) {

            var scope = $rootScope.$new();
            scope.modelId = modelId;

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
.controller('DeleteSegmentController', function ($scope, $rootScope, $state, ResourceUtility, NavUtility, SegmentService) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.deleteSegmentClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        updateAsDeletedSegment(vm.segmentName);
    };

    function updateAsDeletedSegment(segmentName) {
        $("#deleteSegmentError").hide();
        SegmentService.DeleteSegment(segmentName).then(function(result) {
            if (result != null && result.success === true) {
                $("#modalContainer").modal('hide');
                $state.go('home.model.segmentation', {}, { reload: true } );
            } else {
                $scope.deleteSegmentErrorMessage = result.ResultErrors;
                $("#deleteSegmentError").fadeIn();
            }
        });
    }

    $scope.cancelClick = function () {
        $("#modalContainer").modal('hide');
    };

});
