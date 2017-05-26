angular.module('lp.models.notes', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.models.notes.DeleteNoteModal'
])
.controller('NotesController', function ($scope, ResourceUtility, Notes, DeleteNoteModal) {

    var vm = this;
    angular.extend(vm, {
        ResourceUtility: ResourceUtility,
        notes: Notes,
        editingNote: false
    });

    vm.init = function() {

            console.log(vm.notes);

    }
    vm.init();

    vm.addNote = function(note){

    	var newNote = {
            noteId: new Date().getTime(),
            origin: 'Note',
            inherited: false,
            inheritedBy: '',
            createdBy: '',
            createdDateTime: new Date().getTime(),
            body: note,
            edited: false,
            editedBy: '',
            editedDateTime: ''
		};

        NotesService.CreateNote(newNote).then(function(result){

            if (result != null && result.success === true) {
                $state.go('home.model.notes', {}, { reload: true });
            } else {
                vm.saveInProgress = false;
                vm.addNoteErrorMessage = result;
                vm.showAddNoteError = true;
            }

        });

    }

    vm.updateNote = function(noteBody) {
        $scope.body = noteBody;
        $scope.editingNote = false;
    }

    vm.deleteNote = function(noteId) {
        DeleteNoteModal.show(noteId);
	}

});