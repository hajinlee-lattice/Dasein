angular
.module('lp.notes')
.service('NotesService', function($http, $q, $state, $stateParams) {

    this.GetNotes = function(id) {

        var deferred = $q.defer(),
            result,
            id = id || '';
            isRating = $stateParams.rating_id,
            url =  isRating ? '/pls/ratingengines/' + id + '/notes' : '/pls/modelnotes/' + id;

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

    this.CreateNote = function(id, newNote) {
        var deferred = $q.defer(),
            id = id || '',
            isRating = $stateParams.rating_id,
            url =  isRating ? '/pls/ratingengines/' + id + '/notes' : '/pls/modelnotes/' + id,
            data = {
                origin: newNote.Origin,
                user_name: newNote.CreatedByUser,
                content: newNote.NotesContents,
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

    this.UpdateNote = function(id, userName, note) {
        var deferred = $q.defer(),
            id = id || '',
            noteId = note.Id || '',
            userName = userName || '',
            isRating = $stateParams.rating_id,
            url = isRating ? '/pls/ratingengines/' + id + '/notes/' + noteId : '/pls/modelnotes/' + id + '/' + noteId,
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


    this.DeleteNote = function(id, noteId) {
        var deferred = $q.defer(),
            result = {},
            id = id || '',
            noteId = noteId || '',
            isRating = $stateParams.rating_id,
            url = isRating ? '/pls/ratingengines/' + id + '/notes/' + noteId : '/pls/modelnotes/' + id + '/' + noteId;

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
