angular.module('lp.playbook.wizard.name', [])
.component('name', {
    templateUrl: 'app/playbook/content/name/name.component.html',
    bindings: {
        types: '<'
    },
    controller: function(
        $scope, $state, $stateParams, $timeout,
        ResourceUtility, BrowserStorageUtility, PlaybookWizardStore, PlaybookWizardService
    ) {
        var vm = this;
        angular.extend(vm, {
            currentPlay: PlaybookWizardStore.getCurrentPlay(),
            updatedPlay: {}
        });

        var getTypeObj = function(id) {
            var typeObj = {};
            if(id) {
                typeObj = vm.types.filter(function(value) {
                    return (value.id === id);
                });
            }
            return typeObj[0];
        }

        var getTypeByName = function(type) {
            var ret = vm.types.filter(function(value) {
                return (value.displayName === type);
            });
            return ret[0];
        }

        vm.change = function(form) {
            if(vm.updatedPlay.typeId) {
                vm.updatedPlay.playType = getTypeObj(vm.updatedPlay.typeId); // add the type object to the update play (needed to save)
            }
            PlaybookWizardStore.setSettings(vm.updatedPlay);
            vm.checkValid(form);
        }

        vm.$onInit = function() {
            vm.updatedPlay.typeId = getTypeByName('List').id;
            if(vm.currentPlay) {
                if(vm.currentPlay.displayName) {
                    vm.updatedPlay.displayName = vm.currentPlay.displayName;
                }
                if(vm.currentPlay.description) {
                    vm.updatedPlay.description = vm.currentPlay.description;
                }
                if(vm.currentPlay.typeId) {
                    vm.updatedPlay.typeId = vm.currentPlay.typeId;
                }
            }
        }

        vm.checkFieldsDelay = function(form) {
            var mapped = [];
            $timeout(function() {
                vm.checkValid(form);
            }, 1);
        }

        vm.checkValid = function(form) {
            if(form) {
                PlaybookWizardStore.setValidation('name', form.$valid);
            }
        }

}});