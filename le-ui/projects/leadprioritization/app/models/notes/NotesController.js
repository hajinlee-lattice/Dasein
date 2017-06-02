angular.module('lp.models.notes', [
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.models.notes.DeleteNoteModal'
])
.controller('NotesController', function ($scope, $state, $stateParams, $timeout, BrowserStorageUtility, ResourceUtility, Notes, NotesService, DeleteNoteModal) {

    var vm = this,
        ClientSession = BrowserStorageUtility.getClientSession();

    angular.extend(vm, {
        modelId: $stateParams.modelId,
        userName: ClientSession.DisplayName,
        ResourceUtility: ResourceUtility,
        notes: Notes.data,
        editingNote: false,
        showAddNoteError: false,
        saveInProgress: false
    });

    vm.init = function() {

    }
    vm.init();

    vm.addNote = function(note){

    	var newNote = {
            Origin: 'Note',
            CreatedByUser: vm.userName,
            NotesContents: note
		};

        NotesService.CreateNote(vm.modelId, newNote).then(function(result){
            if (result != null && result.success === true) {
                $state.go('home.model.notes', {}, { reload: true });
            } else {
                vm.saveInProgress = false;
                vm.addNoteErrorMessage = result;
                vm.showAddNoteError = true;
            }
        });

    }

    vm.updateNote = function(note) {

        NotesService.UpdateNote(vm.modelId, vm.userName, note).then(function(result){
            if (result != null && result.success === true) {
                $state.go('home.model.notes', {}, { reload: true });
            } else {
                vm.saveInProgress = false;
                vm.addNoteErrorMessage = result;
                vm.showAddNoteError = true;
            }
        });
    }

    vm.deleteNote = function($event, noteId) {
        console.log(noteId);
        DeleteNoteModal.show(vm.modelId, noteId);
	}


});