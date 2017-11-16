angular.module('lp.notes', [
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.notes.DeleteNoteModal',
    'mainApp.models.services.ModelService'
])
.controller('NotesController', function ($scope, $state, $stateParams, $timeout, BrowserStorageUtility, ResourceUtility, Notes, NotesService, DeleteNoteModal, Model, ModelService) {

    var vm = this,
        ClientSession = BrowserStorageUtility.getClientSession();

    angular.extend(vm, {
        isRating: $stateParams.rating_id,
        id: '',
        userName: ClientSession.DisplayName,
        ResourceUtility: ResourceUtility,
        notes: Notes.data,
        referModelName: '',
        editingNote: false,
        showAddNoteError: false,
        saveInProgress: false
    });

    vm.init = function($q) {
        console.log(vm.notes);

        vm.id = vm.isRating ? $stateParams.rating_id : $stateParams.modelId;
        vm.referModelName = vm.isRating ? '' : Model.ModelDetails.Name.slice(0, -7)

    }
    vm.init();

    vm.addNote = function(note){

    	var newNote = {
            Origin: 'Note',
            CreatedByUser: vm.userName,
            NotesContents: note
		};

        NotesService.CreateNote(vm.id, newNote).then(function(result){
            if (result != null && result.success === true) {
                
                if (vm.isRating) {
                    $state.go('home.ratingsengine.dashboard.notes', {}, { reload: true });
                } else {
                    $state.go('home.model.notes', {}, { reload: true });
                }

            } else {
                vm.saveInProgress = false;
                vm.addNoteErrorMessage = result;
                vm.showAddNoteError = true;
            }
        });

    }

    vm.updateNote = function(note) {

        NotesService.UpdateNote(vm.id, vm.userName, note).then(function(result){
            if (result != null && result.success === true) {

                if (vm.isRating) {
                    $state.go('home.ratingsengine.dashboard.notes', {}, { reload: true });
                } else {
                    $state.go('home.model.notes', {}, { reload: true });
                }

            } else {
                vm.saveInProgress = false;
                vm.addNoteErrorMessage = result;
                vm.showAddNoteError = true;
            }
        });
    }

    vm.deleteNote = function($event, noteId) {
        console.log(noteId);
        DeleteNoteModal.show(vm.id, noteId);
	}


});