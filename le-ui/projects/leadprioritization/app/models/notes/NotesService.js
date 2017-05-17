angular
.module('lp.models.notes')
.service('NotesService', function($http, $q, $state) {
    
    this.GetNotes = function(id) {
        var deferred = $q.defer();
        var result;
        var id = id || '';
        var url =  '';

        $http({
            method: 'POST',
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


});
