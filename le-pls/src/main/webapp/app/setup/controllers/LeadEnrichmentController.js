angular.module('mainApp.setup.controllers.LeadEnrichmentController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.core.services.SessionService',
    'mainApp.setup.controllers.LeadEnrichmentAttributesDetailsModel',
    'mainApp.setup.controllers.SaveAttributesModel',
    'mainApp.setup.services.LeadEnrichmentService'
])

.controller('LeadEnrichmentController', function ($scope, $rootScope, $http, _, ResourceUtility, BrowserStorageUtility, NavUtility, SessionService, LeadEnrichmentAttributesDetailsModel, SaveAttributesModel, LeadEnrichmentService) {
    $scope.ResourceUtility = ResourceUtility;
    if (BrowserStorageUtility.getClientSession() == null) { return; }

    load(false);

    function load(showSavingConfirmation) {
        initialize();
        LeadEnrichmentService.GetAvariableAttributes().then(function (avariableAttributesData) {
            if (!avariableAttributesData.Success) {
                handleLoadError(avariableAttributesData);
                return;
            }

            LeadEnrichmentService.GetAttributes().then(function (savedAttributesData) {
                if (!savedAttributesData.Success) {
                    handleLoadError(savedAttributesData);
                    return;
                }

                var availableAttributes = [];
                var selectedAttributes = [];
                var savedAttributes = savedAttributesData.ResultObj;
                var allAvariableAttributes = avariableAttributesData.ResultObj;
                for (var i = 0; i < allAvariableAttributes.length; i++) {
                    var attr = allAvariableAttributes[i];
                    if (getAttribute(savedAttributes, attr.FieldName) != null) {
                        selectedAttributes.push(attr);
                    } else {
                        availableAttributes.push(attr);
                    }
                }
                $scope.allAvariableAttributes = sortAttributes(allAvariableAttributes);
                $scope.availableAttributes = sortAttributes(availableAttributes);
                $scope.selectedAttributes = sortAttributes(selectedAttributes);
                $scope.loading = false;

                if (showSavingConfirmation) {
                    var title = ResourceUtility.getString('LEAD_ENRICHMENT_SETUP_SAVED_ATTRIBUTES_TITLE');
                    LeadEnrichmentAttributesDetailsModel.show($scope, title, $scope.selectedAttributes);
                }
            });
        });
    }

    function initialize() {
        $scope.loading = true;
        $scope.showLoadingError = false;
        $scope.saveInProgress = false;
        $scope.showSaveAttributesError = false;
        $scope.showAttributesDetails = false;
        $scope.showAttributesExcessAlert = false;
        $scope.maxSelectedAttributes = 20;
        $scope.attributesToAdd = [];
        $scope.ctrlAttributesToAdd = [];
        $scope.attributesToRemove = [];
        $scope.ctrlAttributesToRemove = [];
        $scope.selectedAttributes = [];
        $scope.leadEnrichment = $('#leadEnrichment');
        $scope.attributeHover = $('#leadEnrichmentAttributeHover');
    }

    function handleLoadError(data) {
        $scope.loadingError = data.ResultErrors;
        $scope.showLoadingError = true;
        $scope.loading = false;
    }

    $scope.showAllAttributes = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        var title = ResourceUtility.getString('LEAD_ENRICHMENT_SETUP_ALL_ATTRIBUTES_TITLE');
        LeadEnrichmentAttributesDetailsModel.show($scope, title, $scope.allAvariableAttributes);
    };

    $scope.selectedAttributesLabelMouseEnter = function ($event) {
        if ($scope.selectedAttributes.length >= $scope.maxSelectedAttributes) {
            $scope.tooltip = ResourceUtility.getString('LEAD_ENRICHMENT_MAX_SELECTED_ATTRIBUTES_TOOLTIP',
                    [$scope.maxSelectedAttributes]);
            showAttributeHover($($event.currentTarget), $event);
        }
    };

    $scope.selectedAttributesLabelMouseLeave = function ($event) {
        hideAttributeHover();
    };

    $scope.addAttributeButtonClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        if ($('#addLeadEnrichmentAttribute').hasClass('disabled')) {
            return;
        }
        if ($scope.attributesToAdd.length + $scope.selectedAttributes.length > $scope.maxSelectedAttributes) {
            $scope.attributesExcessAlert = ResourceUtility.getString('LEAD_ENRICHMENT_EXCESS_ATTRIBUTES_ALERT',
                    [$scope.attributesToAdd.length, $scope.maxSelectedAttributes]);
            $scope.showAttributesExcessAlert = true;
            return;
        }

        if ($scope.attributesToAdd.length > 0 && $scope.selectedAttributes.length < $scope.maxSelectedAttributes) {
            $scope.showAttributesExcessAlert = false;
            $('#leadEnrichmentAttributeHover').addClass('hide');
            var obj = removeAttributes($scope.availableAttributes, $scope.attributesToAdd);
            $scope.availableAttributes = sortAttributes(obj.remainingAttributes);
            $scope.selectedAttributes = sortAttributes($scope.selectedAttributes.concat(obj.removedAttributes));
            $scope.attributesToAdd = [];
            $scope.ctrlAttributesToAdd = [];
        }
    };

    $scope.removeAttributeButtonClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        if ($scope.attributesToRemove.length > 0) {
            $scope.showAttributesExcessAlert = false;
            $('#leadEnrichmentAttributeHover').addClass('hide');
            var obj = removeAttributes($scope.selectedAttributes, $scope.attributesToRemove);
            $scope.availableAttributes = sortAttributes($scope.availableAttributes.concat(obj.removedAttributes));
            $scope.selectedAttributes = sortAttributes(obj.remainingAttributes);
            $scope.attributesToRemove = [];
            $scope.ctrlAttributesToRemove = [];
        }
    };

    $scope.attributeItemMouseDown = function ($event, isAvailableAttribute) {
        if ($event.which === 3) {
            return;
        }
        if ($event != null) {
            $event.preventDefault();
        }

        var currentItem = $($event.currentTarget);
        var fieldName = getFieldName(currentItem);
        currentItem.addClass('selected');
        if ($event.ctrlKey) {
            var currentIndex = currentItem.index();
            var attributes = isAvailableAttribute ? $scope.attributesToAdd : $scope.attributesToRemove;
            if (attributes.indexOf(fieldName) === -1) {
                attributes.push(fieldName);
            }
            var ctrlAttributes = isAvailableAttribute ? $scope.ctrlAttributesToAdd : $scope.ctrlAttributesToRemove;
            if (ctrlAttributes.length === 0) {
                for (var i = 0; i < attributes.length; i++) {
                    ctrlAttributes.push(attributes[i]);
                }
            } else if (ctrlAttributes.indexOf(fieldName) === -1) {
                ctrlAttributes.push(fieldName);
            }
        } else {
            currentItem.siblings().removeClass('selected');
            if (isAvailableAttribute) {
                $scope.attributesToAdd = [fieldName];
                $scope.ctrlAttributesToAdd = [];
            } else {
                $scope.attributesToRemove = [fieldName];
                $scope.ctrlAttributesToRemove = [];
            }
        }

        if ($scope.selectionStartItem == null) {
            $scope.selectionStartItem = currentItem;
            $(document).mouseup(documentMouseUp);
        }
    };

    function documentMouseUp() {
        $scope.selectionStartItem = null;
        $(document).unbind("mouseup", documentMouseUp);
    }

    $scope.attributeItemMouseEnter = function ($event, isAvailableAttribute) {
        if ($event != null) {
            $event.preventDefault();
        }

        var currentItem = $($event.currentTarget);
        if ($scope.selectionStartItem != null) {
            var list = currentItem.parent();
            if (list.attr("id") === $scope.selectionStartItem.parent().attr("id")) {
                var startIndex = $scope.selectionStartItem.index();
                var endIndex = currentItem.index();
                if (startIndex > endIndex) {
                    var temp = endIndex;
                    endIndex = startIndex;
                    startIndex = temp;
                }

                var selectedItems = [];
                var allItems = list.find('li');
                var ctrlAttributes = isAvailableAttribute ? $scope.ctrlAttributesToAdd : $scope.ctrlAttributesToRemove;
                allItems.each(function (index) {
                    var item = $(this);
                    if (index >= startIndex && index <= endIndex) {
                        selectedItems.push(getFieldName(item));
                        item.addClass('selected');
                    } else {
                        var fieldName = getFieldName(item);
                        if (ctrlAttributes.indexOf(fieldName) > -1) {
                            selectedItems.push(fieldName);
                            item.addClass('selected');
                        } else {
                            item.removeClass('selected');
                        }
                    }
                });
                if (isAvailableAttribute) {
                    $scope.attributesToAdd = selectedItems;
                } else {
                    $scope.attributesToRemove = selectedItems;
                }
            }
        } else {
            var attributes = isAvailableAttribute ? $scope.availableAttributes : $scope.selectedAttributes;
            var attr = getAttribute(attributes, getFieldName(currentItem));
            if (attr != null && attr.Description != null && attr.Description.length > 0) {
                $scope.tooltip = attr.Description;
                showAttributeHover(currentItem, $event);
            }
        }
    };

    $scope.attributeItemMouseLeave = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        hideAttributeHover();
    };

    $scope.saveAttributesButtonClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.showSaveAttributesError = false;
        if ($scope.selectedAttributes.length > $scope.maxSelectedAttributes) {
            $scope.attributesExcessAlert = ResourceUtility.getString('LEAD_ENRICHMENT_CANNOT_SAVE_FOR_EXCESS_ATTRIBUTES', $scope.maxSelectedAttributes);
            $scope.showAttributesExcessAlert = true;
            return;
        }

        SaveAttributesModel.show($scope);
    };

    $scope.saveAttributes = function () {
        $scope.saveInProgress = true;
        var attributes = [];
        for (var i = 0; i < $scope.selectedAttributes.length; i++) {
            var attribute = $scope.selectedAttributes[i];
            attributes[i] = { FieldName: attribute.FieldName, DataSource: attribute.DataSource };
        }
        LeadEnrichmentService.SaveAttributes(attributes).then(function (data) {
            if (data.Success) {
                $scope.saveInProgress = false;
                load(true);
            } else {
                $scope.saveAttributessErrorMessage = data.ResultErrors;
                $scope.showSaveAttributesError = true;
                $scope.saveInProgress = false;
            }
        });
    };

    $scope.setAttributesDetails = function (value) {
        $scope.showAttributesDetails = value;
    };

    function removeAttributes(attributes, attributesToRemove) {
        var removedAttributes = [];
        for (var i = 0; i < attributesToRemove.length; i++) {
            var attrToRemove = getAttribute(attributes, attributesToRemove[i]);
            if (attrToRemove != null) {
                removedAttributes.push(attrToRemove);
            }
        }
        var remainingAttributes = [];
        for (var j = 0; j < attributes.length; j++) {
            var attr = attributes[j];
            if (_.indexOf(attributesToRemove, attr.FieldName) === -1) {
                remainingAttributes.push(attr);
            }
        }
        return { removedAttributes: removedAttributes, remainingAttributes: remainingAttributes };
    }

    function getAttribute(attributes, fieldName) {
        return _.findWhere(attributes, { FieldName: fieldName });
    }

    function sortAttributes(attributes) {
        return _.sortBy(attributes, 'DisplayName');
    }

    function getFieldName(attributeItem) {
        return attributeItem.attr('fn');
    }

    function showAttributeHover(target, $event) {
        $scope.attributeHoverTimeout = setTimeout(function() {
            var offset = target.offset();
            if (offset.top <= 0) {
                return;
            }

            var container = $scope.leadEnrichment;
            var top = offset.top + target.height() + container.scrollTop() - container.position().top + 10;
            if (top <= 0) {
                return;
            }

            var hover = $scope.attributeHover;
            hover[0].style.opacity = 0;
            hover.removeClass('hide');
            hover.css('top', top);
            var left = $event.pageX + container.scrollLeft() - parseInt(hover.width() / 3);
            var minLeft = offset.left + 20;
            if (left < minLeft) {
                left = minLeft;
            }
            hover.css('left', left);
            hover[0].style.opacity = 1;
        }, 400);
    }

    function hideAttributeHover() {
        if ($scope.attributeHoverTimeout) {
            clearTimeout($scope.attributeHoverTimeout);
        }
        $scope.attributeHover.addClass('hide');
    }
});