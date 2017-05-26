angular
.module('lp.models.notes')
.service('NotesService', function($http, $q, $state) {
    
    this.GetNotes = function(id) {
        var deferred = $q.defer();
        var result;
        var id = id || '';
        var url =  '/pls/modelnotes/' + id;

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Content-Type': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                var result = {
                    data: response.data,
                    success: true
                };
                
                deferred.resolve(result);

            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.CreateNote = function(modelId, newNote) {
        var deferred = $q.defer(),
            id = modelId || '',
            data = {
                name: newNote.noteId,
                origin: newNote.origin,
                inherited: newNote.inherited,
                inheritedBy: newNote.inheritedBy,
                createdBy: newNote.createdBy,
                createdDateTime: newNote.createdDateTime,
                body: newNote.body,
                edited: newNote.edited,
                editedBy: newNote.editedBy,
                editedDateTime: newNote.editedDateTime
            };

        $http({
            method: 'POST',
            url: '/pls/modelnotes/' + id,
            data: data,
            headers: { 'Content-Type': 'application/json' }
        }).then(
            function onSuccess(response) {
                var result = {
                    data: response.data,
                    success: true
                };
                
                deferred.resolve(result);

            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.UpdateNote = function(noteId, note) {
        var deferred = $q.defer(),
            id = modelId || '',
            data = {
                name: note.noteId,
                origin: note.origin,
                inherited: note.inherited,
                inheritedBy: note.inheritedBy,
                createdBy: note.createdBy,
                createdDateTime: note.createdDateTime,
                body: note.body,
                edited: note.edited,
                editedBy: note.editedBy,
                editedDateTime: note.editedDateTime
            };

        $http({
            method: 'POST',
            url: '/pls/modelnotes/' + id,
            data: data,
            headers: { 'Content-Type': 'application/json' }
        }).then(
            function onSuccess(response) {
                var result = {
                    data: response.data,
                    success: true
                };
                
                deferred.resolve(result);

            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );

        return deferred.promise;
    }


    this.DeleteNote = function(noteId) {
        var deferred = $q.defer(),
            result = {},
            url = '/pls/modelnotes/' + noteId;

        $http({
            method: 'DELETE',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                result = {
                    data: response.data,
                    success: true
                };
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.reject(errorMsg);
            }
        );

        return deferred.promise;
    }


});
