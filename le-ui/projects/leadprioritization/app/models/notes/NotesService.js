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
                origin: newNote.Origin,
                user_name: newNote.CreatedByUser,
                content: newNote.NotesContents,
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

    this.UpdateNote = function(modelId, userName, note) {
        var deferred = $q.defer(),
            modelId = modelId || '',
            noteId = note.Id || '',
            userName = userName || '',
            url = '/pls/modelnotes/' + modelId + '/' + noteId,
            data = {
                origin: note.Origin,
                user_name: userName,
                content: note.NotesContents
            };

        $http({
            method: 'POST',
            url: url,
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


    this.DeleteNote = function(modelId, noteId) {
        var deferred = $q.defer(),
            result = {},
            modelId = modelId || '',
            noteId = noteId || '',
            url = '/pls/modelnotes/' + modelId + '/' + noteId;

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
