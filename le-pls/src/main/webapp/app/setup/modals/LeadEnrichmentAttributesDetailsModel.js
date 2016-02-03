angular.module('mainApp.setup.controllers.LeadEnrichmentAttributesDetailsModel', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.core.services.SessionService',
    'mainApp.setup.services.LeadEnrichmentService'
])

.service('LeadEnrichmentAttributesDetailsModel', function ($compile, $rootScope, $http) {
    this.show = function($parentScope, title, attributes) {
        $http.get('./app/setup/views/LeadEnrichmentAttributesDetailsView.html').success(function (html) {
            var scope = $parentScope.$new();
            scope.title = title;
            scope.originalAttributes = attributes;
            scope.hasAttributes = (attributes != null && attributes.length > 0);
            var contentContainer = $('#leadEnrichmentAttributesDetails');
            $compile(contentContainer.html(html))(scope);
        });
    };
})

.controller('LeadEnrichmentAttributesDetailsController', function ($scope, $rootScope, $http, _, ResourceUtility, BrowserStorageUtility, NavUtility, SessionService, ConfigService, LeadEnrichmentService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.$parent.setAttributesDetails(true);

    if ($scope.hasAttributes) {
        var templateType = $('#leadEnrichmentAttributesDetails').attr('templatetype');
        if (templateType != null && templateType !== '') {
            generateAttributes(templateType);
        } else {
            $scope.loading = true;
            LeadEnrichmentService.GetTemplateType().then(function(result) {
                if (result.Success === true) {
                    $('#leadEnrichmentAttributesDetails').attr('templatetype', result.ResultObj);
                    generateAttributes(result.ResultObj);
                } else {
                    $scope.showLoadingError = true;
                    $scope.loadingError = result.ResultErrors;
                }
                $scope.loading = false;
            });
        }
    }

    function generateAttributes(templateType) {
        var attributes = [$scope.originalAttributes.length];
        for (var i = 0; i < $scope.originalAttributes.length; i++) {
            var attribute = {};
            var originalAttribute = $scope.originalAttributes[i];
            attribute.name = originalAttribute.DisplayName;
            attribute.fieldName = 'Lattice_' + originalAttribute.FieldName;
            attribute.fieldType = convertFieldType(templateType, originalAttribute.FieldType);
            attributes[i] = attribute;
        }
        $scope.attributes = attributes;
    }

    function convertFieldType(templateType, fieldType) {
        var dataType = fieldType;
        if (dataType != null) {
            if (templateType != null && templateType.toLowerCase() === 'mkto') {
                dataType = dataType.replace(/NVARCHAR/i, 'String');
                dataType = dataType.replace(/VARCHAR/i, 'String');
                dataType = dataType.replace(/INT/i, 'Integer');
                dataType = dataType.replace(/FLOAT/i, 'Float');
            } else {
                dataType = dataType.replace(/NVARCHAR/i, 'Text');
                dataType = dataType.replace(/VARCHAR/i, 'Text');
                dataType = dataType.replace(/INT/i, 'Number');
                dataType = dataType.replace(/FLOAT/i, 'Number');
            }
        }
        return dataType;
    }

    $scope.backButtonClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.attributes = null;
        $scope.originalAttributes = null;
        $scope.$parent.setAttributesDetails(false);
    };
});