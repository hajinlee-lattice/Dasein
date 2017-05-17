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

    	vm.notes.push(newNote);
    }

    vm.editNote = function(noteId){
        vm.editingNote = true;
    }

    vm.cancelEditNote = function($event, $index){
    	vm.editingNote = false;	
    }

    vm.deleteNote = function(noteId) {
        DeleteNoteModal.show(noteId);
	}

});