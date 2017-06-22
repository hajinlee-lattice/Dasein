angular.module('mainApp.models.remodel', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.models.modals.BasicConfirmation',
    'mainApp.models.modals.RemodelingModal',
    'mainApp.setup.services.MetadataService',
    'mainApp.appCommon.utilities.StringUtility',
    'lp.models.remodel',
    'lp.jobs'
])
.controller('RemodelController', function($scope, $filter, $state, $timeout, $anchorScroll, MetadataService,
        RemodelTooltipService, RemodelStore, Model, DataRules, Attributes, BasicConfirmationModal,
        RemodelingModal, ResourceUtility, StringUtility, HealthService
) {

    if (Model.ModelType === 'PmmlModel' ||
        Model.ModelDetails.Uploaded === true) {
        backToModel();
        return;
    }

    var vm = this;
    angular.extend(vm, {
        messageTitle: null,
        message: null,
        successs: null,
        error: null,
        disableRemodelOnError: false,
        dataRulesMap: {},
        attributes: [],
        newModelName: Model.ModelDetails.DisplayName.replace(/\s+/g, '_') + $filter('date')(new Date().getTime(), '_yyyyMMdd') + '_Remodel',
        notesContent: '',
        isDirty: false,
        remodeling: false,
        sortBy: 'name',
        sortAsc: true
    });

    vm.init = function($q) {
        
    }
    vm.init();

    vm.toggle = function(attribute) {
        if (vm.remodeling) { return; }

        vm.isDirty = true;

        if (attribute.attribute.ApprovedUsage === 'None') {
            attribute.attribute.ApprovedUsage = 'ModelAndAllInsights';
            attribute.value = true;
        } else {
            attribute.attribute.ApprovedUsage = 'None';
            attribute.value = false;
        }
    };

    for (var i = 0; i < DataRules.length; i++) {
        var dataRule = DataRules[i];
        vm.dataRulesMap[dataRule.name] = dataRule;
    }

    if (typeof Attributes === "string") {
        vm.attributes = [];
        vm.disableRemodelOnError = true;
    } else {
        vm.attributes = Attributes.map(function(attribute) {
            var attributeObj = {
                name: attribute.DisplayName,
                value: attribute.ApprovedUsage !== 'None',
                recommendations: [],
                disabled: false,
                warning: null,
                attribute: angular.copy(attribute)
            };

            if (attribute.IsCoveredByMandatoryRule) {
                attributeObj.disabled = true;
                attributeObj.warning = 'mandatory';
            } else if (attribute.IsCoveredByOptionalRule) {
                attributeObj.disabled = false;
                attributeObj.warning = 'optional';
            }

            if (attribute.AssociatedRules) {
                for (var i = 0; i < attribute.AssociatedRules.length; i++) {
                    var associatedRule = attribute.AssociatedRules[i];

                    var dataRuleList = vm.dataRulesMap[associatedRule];
                    if (dataRuleList) {
                        attributeObj.recommendations.push(dataRuleList);
                    }
                }
            }

            return attributeObj;
        });
    }

    vm.sort = function(sortBy) {
        if (vm.sortBy === sortBy) {
            vm.sortAsc = !vm.sortAsc;
        } else {
            vm.sortBy = sortBy;
        }
    };

    vm.checkStatusBeforeRemodel = function() {
        HealthService.checkSystemStatus().then(function() {
            vm.remodel();
        }).catch(function() {
            $anchorScroll();
        });
    };

    vm.remodel = function() {
        var modelNameFormatted = StringUtility.SubstituteAllSpecialCharsWithDashes(vm.newModelName);

        var copy_text = " (copy)",
            oneLeadPerDomain = Model.EventTableProvenance.Is_One_Lead_Per_Domain == null ? false :
                Model.EventTableProvenance.Is_One_Lead_Per_Domain == "true",
            dedupType = oneLeadPerDomain ? 'ONELEADPERDOMAIN' : 'MULTIPLELEADSPERDOMAIN',
            includePersonalEmailDomains = Model.EventTableProvenance.Exclude_Public_Domains == null ? true :
                Model.EventTableProvenance.Exclude_Public_Domains == "false",
            useLatticeAttributes = Model.EventTableProvenance.Exclude_Propdata_Columns == null ? true :
                Model.EventTableProvenance.Exclude_Propdata_Columns == "false",
            enableTransformations = (Model.EventTableProvenance.Transformation_Group_Name == "none" ||
                Model.ModelDetails.TransformationGroupName === "none") ? false : true,
            modelName = modelNameFormatted,
            modelDisplayName = vm.newModelName,
            notesContent = vm.notesContent,
            originalModelSummaryId = Model.ModelDetails.ModelID,
            fields = vm.attributes.map(function(attribute) {
                return attribute.attribute;
            });

        vm.remodeling = true;
        RemodelingModal.show();

        MetadataService.UpdateAndCloneFields(dedupType, includePersonalEmailDomains, useLatticeAttributes,
                enableTransformations, modelName, modelDisplayName, notesContent, originalModelSummaryId, fields, null).then(function(result) {

            if (result.Success === true) {
                vm.success = true;
                vm.messageTitle = ResourceUtility.getString('MODEL_REMODEL_REMODELING_TITLE');
                vm.message = ResourceUtility.getString('MODEL_REMODEL_REMODELING_MESSAGE') + ' Now redirecting to jobs page...';

                vm.isDirty = false;
            } else {
                vm.error = true;
                vm.messageTitle = 'Error';
                vm.message = result.ResultErrors;
            }
        }).finally(function() {
            RemodelingModal.hide();
            vm.remodeling = false;
            if (vm.success) {
                $state.go('home.jobs.status', { 'jobCreationSuccess': true });
            }
        });
    };

    function backToModel() {
        $state.go('home.model.attributes', {modelId: Model.ModelDetails.ModelID});
    }

    vm.cancel = function() {
        if (vm.isDirty) {
            var title = ResourceUtility.getString('SETUP_CANCEL_CONFIRM_TITLE');
            var text = ResourceUtility.getString('MODEL_REMODEL_CANCEL_MESSAGE');
            var confirmButtonLabel = ResourceUtility.getString('BUTTON_DISCARD_CHANGES_LABEL');

            BasicConfirmationModal.show(title, text, confirmButtonLabel, null, backToModel, null);
        } else {
            backToModel();
        }
    };

    vm.closeMessage = function() {
        vm.message = null;
        vm.messageTitle = null;
        vm.successs = null;
        vm.error = null;
    };

    var timeout = null;
    var tooltipEl = angular.element('#remodel-tooltip');
    tooltipEl.mouseenter(function() {
        $timeout.cancel(timeout);
    });
    tooltipEl.mouseleave(function() {
        vm.hideTooltip(true);
    });

    vm.showTooltip = function($event, attribute) {
        $timeout.cancel(timeout);
        RemodelTooltipService.show(tooltipEl, $event, attribute);
    };

    vm.hideTooltip = function(immediate) {
        if (immediate === true) {
            RemodelTooltipService.hide(tooltipEl);
        } else {
            timeout = $timeout(function() {
                RemodelTooltipService.hide(tooltipEl);
            }, 300, false);
        }
    };
})
.service('RemodelTooltipService', function() {
    var RemodelTooltipService = this;

    this.show = function(el, $event, attribute) {
        var template = RemodelTooltipService.template(attribute);

        var left = $event.target.offsetParent.offsetLeft - el.outerWidth(),
            top = $event.target.offsetParent.offsetTop;

        el.html(template);
        el.css({top: top, left: left});
        el.removeClass(function(index, css) {
            return (css.match(/(^|\s)warn-\S+/g) || []).join(' ');
        });
        el.addClass('warn-' + attribute.warning);
        el.addClass('active');
    };

    this.hide = function(el) {
        el.removeClass('active');
    };

    this.template = function(attribute) {
        var template = attribute.recommendations.reduce(function(tmpl, rec, i) {
            var header = '<div class="remodel-tooltip-title">' + rec.displayName + '</div>';
            var sub = '<div>Recommendation: <span class="remodel-tooltip-rec">Exclude</span></div>'
            var body = '<p>' + rec.description + '</p>'

            return tmpl + (i > 0 ? '<hr>' : '') + '<div>' + header + sub + body + '</div>';
        }, '');

        return '<div class="remodel-tooltip-inner">' + template + '</div>';
    };
});
