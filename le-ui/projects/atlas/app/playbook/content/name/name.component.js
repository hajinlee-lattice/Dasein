angular.module('lp.playbook.wizard.name', [])
.component('name', {
    templateUrl: 'app/playbook/content/name/name.component.html',
    bindings: {
        types: '<'
    },
    controller: function(
        $scope, $state, $stateParams,
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

        vm.change = function() {
            if(vm.updatedPlay.typeId) {
                vm.updatedPlay.playType = getTypeObj(vm.updatedPlay.typeId); // add the type object to the update play (needed to save)
            }
            PlaybookWizardStore.setSettings(vm.updatedPlay);
        }

        vm.$onInit = function() {
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

    }});