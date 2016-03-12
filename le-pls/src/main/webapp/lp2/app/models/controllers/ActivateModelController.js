angular.module('mainApp.models.controllers.ActivateModelController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.models.services.ModelService',
    'mainApp.models.modals.AddSegmentModal'
])

.controller('ActivateModelController', function ($scope, BrowserStorageUtility, ResourceUtility, ModelService, AddSegmentModal) {
    $scope.ResourceUtility = ResourceUtility;
    if (BrowserStorageUtility.getClientSession() == null) { 
        return; 
    }
    
    $scope.loading = true;
    $scope.segments = [];
    $scope.showError = false;
    $scope.errorMessage = "";
    $scope.saving = false;
    // Can't Add or Delete segments. Leaving this in case we want to reactivate it in the future.
    $scope.allowAddAndDelete = false;
    
    $scope.closeErrorClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        $scope.showError = false;
    };
    
    $scope.models = [{
        Id: "FAKE_MODEL",
        DisplayName: "Select"
    }];
    
    function sortByPriority (a, b) {
        if (a.Priority < b.Priority) {
            return -1;
        }
        if (a.Priority > b.Priority) {
            return 1;
        }
        // a must be equal to b
        return 0;
    }
    
    ModelService.GetAllModels(true).then(function(result) {
        // Get model list, but it may be empty
        if (result != null && result.success === true) {
            $scope.models = $scope.models.concat(result.resultObj);
        }
        
        loadSegments();
    });
    
    function loadSegments() {
        ModelService.GetAllSegments($scope.models).then(function(result) {
            $scope.loading = false;                    
            $scope.saving = false;        
            if (result != null && result.success === true) {
                for (var i=0;i<result.resultObj.length;i++){
                    //Need this to verify changes against the model in the list
                    result.resultObj[i].NewModelId = result.resultObj[i].ModelId;
                }
                $scope.segments = result.resultObj.sort(sortByPriority);
            } else {
                $scope.showError = true;
                $scope.errorMessage = ResourceUtility.getString("ACTIVATE_MODEL_GET_SEGMENTS_ERROR");
            }
        });        
    }

    $scope.saveSegmentsClicked = function () {
        if ($scope.saving) {
            return;
        }    
        $scope.saving = true;    
        ModelService.UpdateSegments($scope.segments).then(function(result) {       
            if (result != null && result.success === true) {
                // pass
            } else if (result.success === false) {              
                $scope.showError = true;
                $scope.errorMessage = result.resultErrors;
            } else {
                $scope.showError = true;
                $scope.errorMessage = ResourceUtility.getString("ACTIVATE_MODEL_UPDATE_SEGMENTS_ERROR");              
            }
            loadSegments();         
        });
    };

    $scope.addNewSegmentClicked = function () {
        AddSegmentModal.show($scope.segments, $scope.models, function (segment) {
            if (segment != null) {
                if (segment.ModelId == null) {
                    segment.ModelId = "FAKE_MODEL";
                }
                segment.NewModelId = segment.ModelId;
                $scope.segments.push(segment);
            } 
        });
    };
    
    $scope.deleteSegmentClicked = function (segment) {
        if (segment == null) {
            return;
        }
        ModelService.DeleteSegment(segment.Name).then(function(result) {
            if (result != null && result.success === true) {
                for (var i=0;i<$scope.segments.length;i++) {
                    if (segment.Name == $scope.segments[i].Name) {
                        $scope.segments.splice(i, 1);
                        break;
                    }
                }
            } else {
                $scope.showError = true;
                $scope.errorMessage = ResourceUtility.getString("ACTIVATE_MODEL_DELETE_SEGMENT_ERROR", [segment.Name]);
            }
        });
        
    };
    
    function updateSegment(segment, successCallback, failCallback) {
        if (successCallback) {
            successCallback();
        }
    }
    
    $scope.modelChanged = function (segment) {
        if (segment.ModelId == segment.NewModelId) {
            return;
        }
        
        var model = null;
        for (var i=0;i<$scope.models.length;i++){
            if (segment.NewModelId == $scope.models[i].Id) {
                model = $scope.models[i];
                break;
            }
        }
        
        if (model == null) {
            return;
        }
        
        var backupSegment = $.extend(true, {}, segment);
        var secondSegment = null;
        var backupSecondSegment = null;
        if (segment.NewModelId == "FAKE_MODEL") {
            segment.ModelId = null;
            segment.ModelName = null;
        } else {
            segment.ModelId = model.Id;
            segment.ModelName = model.DisplayName;
            
            // 20150603 BKN - Model to Segment is 1:1. Do not allow model assignment to multiple segments.
            // now we need to figure out if that model was associated to another segment and remove it
            for (var x=0;x<$scope.segments.length;x++) {
                if (segment.Name != $scope.segments[x].Name && $scope.segments[x].ModelId == model.Id) {
                    secondSegment = $scope.segments[x];
                    break;
                }
            }        
            
            if (secondSegment != null) {
                backupSecondSegment = $.extend(true, {}, secondSegment);
                secondSegment.ModelId = null;
                secondSegment.ModelName = null;
                secondSegment.NewModelId = "FAKE_MODEL";
            }
        }
        
        // Save updated segments here
        if (secondSegment) {
            updateSegment(secondSegment, function () {
                updateSegment(segment, null, function () {
                    // Undo changes if a failure occurs
                    $.extend(true, segment, backupSegment);
                });
            }, function () {
                // Undo changes if a failure occurs
                $.extend(true, secondSegment, backupSecondSegment);
                $.extend(true, segment, backupSegment);
            });
        } else {
            updateSegment(segment, null, function () {
                // Undo changes if a failure occurs
                $.extend(true, segment, backupSegment);
            });
        }
    };
    
    $scope.showDecreasePriority = function (segment) {
        if (segment == null) {
            return false;
        }
        
        if (segment.Name == 'LATTICE_DEFAULT_SEGMENT') {
            return false;
        }

        if (segment.Priority >= $scope.segments.length-1) {
            return false;
        }

        for (var i=0;i<$scope.segments.length;i++) {
            var toCheck = $scope.segments[i];
            if (segment.Name != toCheck.Name && segment.Priority < toCheck.Priority) {
                return true;
            }
        }
        return false;
    };
    
    $scope.showIncreasePriority = function (segment) {
        if (segment == null) {
            return false;
        }
        
        if (segment.Name == 'LATTICE_DEFAULT_SEGMENT') {
            return false;
        }

        for (var i=0;i<$scope.segments.length;i++) {
            var toCheck = $scope.segments[i];
            if (segment.Name != toCheck.Name && segment.Priority > toCheck.Priority) {
                return true;
            }
        }
        return false;
    };
    
    $scope.decreasePriorityClicked = function ($event, segment) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        if (segment == null) {
            return;
        }
        
        var nextLowest = null;
        for (var i=0;i<$scope.segments.length;i++) {
            if (segment.Name == $scope.segments[i].Name) {
                var next = i + 1;
                nextLowest = $scope.segments[next];
                break;
            }
        }
        
        var currentPriority = segment.Priority;
        if (nextLowest) {
            segment.Priority = nextLowest.Priority;
            nextLowest.Priority = currentPriority;
            $scope.segments = $scope.segments.sort(sortByPriority);
            updateSegment(segment, function () {
                updateSegment(nextLowest, function () {
                    $scope.segments = $scope.segments.sort(sortByPriority);
                }, function () {
                    // Undo changes if a failure occurs
                    segment.Priority = currentPriority;
                    $scope.segments = $scope.segments.sort(sortByPriority);
                });
            }, function () {
                // Undo changes if a failure occurs
                nextLowest.Priority = segment.Priority;
                segment.Priority = currentPriority;
                $scope.segments = $scope.segments.sort(sortByPriority);
            });
        }
        
    };
    
    $scope.increasePriorityClicked = function ($event, segment) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        if (segment == null) {
            return;
        }
        
        var nextHighest = null;
        for (var i=0;i<$scope.segments.length;i++) {
            if (segment.Name == $scope.segments[i].Name) {
                var previous = i - 1;
                nextHighest = $scope.segments[previous];
                break;
            }
        }
        
        var currentPriority = segment.Priority;
        if (nextHighest) {
            segment.Priority = nextHighest.Priority;
            nextHighest.Priority = currentPriority;
            $scope.segments = $scope.segments.sort(sortByPriority);
            updateSegment(segment, function () {
                updateSegment(nextHighest, function () {
                    $scope.segments = $scope.segments.sort(sortByPriority);
                }, function () {
                    // Undo changes if a failure occurs
                    segment.Priority = currentPriority;
                    $scope.segments = $scope.segments.sort(sortByPriority);
                });
            }, function () {
                // Undo changes if a failure occurs
                nextHighest.Priority = segment.Priority;
                segment.Priority = currentPriority;
                $scope.segments = $scope.segments.sort(sortByPriority);
            });
        }
    };
});