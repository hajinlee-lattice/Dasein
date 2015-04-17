angular.module('mainApp.models.modals.AddSegmentModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.models.services.ModelService'
])

.service('AddSegmentService', function (StringUtility) {
    
    this.GetLowestPriorityAvailable = function (segments) {
        if (segments == null || segments.length === 0) {
            return 1;
        }
        var lowestPriority = null;
        for (var i=0;i<segments.length;i++) {
            if (lowestPriority == null) {
                lowestPriority = segments[i].Priority + 1;
            } else if (segments[i].Priority < lowestPriority) {
                lowestPriority = segments[i].Priority + 1;
            }
            
        }
        return lowestPriority;
    };
    
    this.GetUnassociatedModels = function (segments, models) {
        var toReturn = [];
        if (segments == null || segments.length === 0 || models == null || models.length === 0) {
            return toReturn;
        }
        
        var segmentDict = {};
        for (var i=0;i<segments.length;i++) {
            segmentDict[segments[i].ModelId] = 1;
        }
        
        while (models.length > 0) {
            var model = models.shift();
            if (segmentDict[model.Id] == null) {
                toReturn.push(model);
            }
        }
    };
    
    this.ValidateSegmentName = function (name, segments) {
        if (StringUtility.IsEmptyString(name)) {
            return false;
        }
        
        if (segments == null || segments.length === 0) {
            return true;
        }
        
        for (var i=0;i<segments.length;i++) {
            var segmentName = segments[i].Name.trim();
            if (segmentName.toLowerCase() == name.toLowerCase()) {
                return false;
            }
        }
        
        return true;
    };
})

.service('AddSegmentModal', function ($compile, $rootScope, $http) {
    this.show = function (segments, models) {
        $http.get('./app/models/views/AddSegmentView.html').success(function (html) {
            
            var scope = $rootScope.$new();
            scope.segments = segments;
            scope.models = models;

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

.controller('AddSegmentController', function ($scope, $rootScope, ResourceUtility, ModelService, AddSegmentService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.saveInProgress = false;
    $scope.showAddSegmentSuccess = false;
    $scope.addSegmentSuccessMessage = "";
    $scope.showAddSegmentError = false;
    $scope.addSegmentErrorMessage = "";
    
    var segments = $scope.segments;
    $scope.filteredModels = AddSegmentService.GetUnassociatedModels(segments, $scope.models);
    
    $scope.newSegment = {
        Name: null,
        Priority: AddSegmentService.GetLowestPriorityAvailable(segments),
        ModelId: ""
    };

    $scope.addSegmentClick = function () {
        $scope.showAddSegmentSuccess = false;
        $scope.addSegmentSuccessMessage = "";
        $scope.addSegmentErrorMessage = "";
        $scope.showAddSegmentError = false;
        var isValid = AddSegmentService.ValidateSegmentName($scope.newSegment.Name, segments);
        if (isValid) {
            $scope.saveInProgress = true;
            ModelService.AddSegment($scope.newSegment).then(function(result) {
                $scope.saveInProgress = false;
                if (result && result.success === true) {
                    $("#modalContainer").modal('hide');
                } else {
                    $scope.addSegmentErrorMessage = result.resultErrors;
                    $scope.showAddSegmentError = true;
                }
            });
        } else {
            $scope.addSegmentErrorMessage = ResourceUtility.getString('MULTIPLE_MODEL_ADD_SEGMENT_NAME_ERROR');
            $scope.showAddSegmentError = true;
        }
    };
    
    $scope.cancelClick = function () {
        $("#modalContainer").modal('hide');
    };
    
});