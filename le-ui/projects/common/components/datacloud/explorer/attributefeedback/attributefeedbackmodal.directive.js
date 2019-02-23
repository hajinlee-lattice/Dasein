export default function () {
    return {
        restrict: 'EA',
        templateUrl: '/components/datacloud/explorer/attributefeedback/attributefeedbackmodal.component.html',
        scope: {
            lookupResponse: '=?',
        },
        controller: function (
            $scope, $document, $timeout, LookupStore, DataCloudStore, BrowserStorageUtility
        ) {
            'ngInject';

            // test on LETest1503428538807_LPI
            var vm = $scope,
                lookupStore = LookupStore.get('request'),
                $modal = angular.element('attribute-feedback-modal');

            $scope.modal = DataCloudStore.getFeedbackModal();
            $scope.close = function () {
                DataCloudStore.setFeedbackModal(false);
            }

            var clientSession = BrowserStorageUtility.getClientSession();

            $scope.showLookupStore = $scope.modal.context.showLookupStore;
            if ($scope.showLookupStore) {
                $scope.lookupStore = lookupStore;
            }

            $scope.icon = $scope.modal.context.icon;
            $scope.label = $scope.modal.context.label || $scope.modal.context.attribute.DisplayName;
            $scope.value = $scope.modal.context.value;
            $scope.categoryClass = $scope.modal.context.categoryClass;
            $scope.reported = false;

            $scope.report = function () {
                $scope.sendingReport = true;
                var form_fields = $scope.input || {}
                var report = {
                    Attribute: $scope.modal.context.attributeKey || $scope.modal.context.attribute.ColumnId || '',
                    MatchedValue: $scope.value,
                    CorrectValue: form_fields.value || '',
                    Comment: form_fields.comment || '',
                    InputKeys: lookupStore.record,
                    MatchedKeys: {
                        "LDC_Name": $scope.lookupResponse.attributes.LDC_Name,
                        "LDC_Domain": $scope.lookupResponse.attributes.LDC_Domain,
                        "LDC_City": $scope.lookupResponse.attributes.LDC_City,
                        "LDC_State": $scope.lookupResponse.attributes.LDC_State,
                        "LDC_ZipCode": $scope.lookupResponse.attributes.LDC_ZipCode,
                        "LDC_Country": $scope.lookupResponse.attributes.LDC_Country,
                        "LDC_DUNS": $scope.lookupResponse.attributes.LDC_DUNS
                    },
                    MatchLog: $scope.lookupResponse.matchLogs
                };

                $scope.sendingReport = false;
                $scope.reported = true;
                $timeout(function () {
                    $scope.close();
                }, 1000 * 5);
                DataCloudStore.sendFeedback(report, $scope.modal.context.type);
            }

            var setHeight = function () {
                var height = $(window).height() - 20;
                $modal.css({ maxHeight: height });
                $modal.find('.attribute-feedback-container').css({ maxHeight: height });
            }

            $scope.setHeight = function () { // gets called by ng-inits so that the data is there and heights make sense
                setHeight();
            }

            var _handleDocumentResize = _.debounce(handleDocumentResize, 300);
            $(window).on('resize', _handleDocumentResize);

            function handleDocumentResize(evt) {
                setHeight();
            }

            $document.on('click', handleDocumentClick);

            function handleDocumentClick(evt) {
                var target = angular.element(evt.target),
                    clickedModal = target.parents('attribute-feedback-modal').length;

                if (!clickedModal) {
                    $scope.close();
                    $scope.$apply();
                }
            }

            $scope.$on('$destroy', function () {
                $document.off('click', handleDocumentClick);
                $(window).off('resize', _handleDocumentResize)
            });
        }
    };
};