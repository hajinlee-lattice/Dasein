angular.module('mainApp.notes.DeleteNoteModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.utilities.NavUtility'
])
.service('DeleteNoteModal', function ($compile, $templateCache, $rootScope, $http, ResourceUtility, NotesService) {
    var self = this;
    this.show = function (id, noteId) {
        $http.get('app/notes/DeleteNoteConfirmView.html', { cache: $templateCache }).success(function (html) {

            var scope = $rootScope.$new();
            scope.noteId = noteId;
            scope.id = id;

            var modalElement = $("#modalContainer");
            $compile(modalElement.html(html))(scope);

            scope.hasDeleteError = false;

            var options = {
                backdrop: "static"
            };
            modalElement.modal(options);
            modalElement.modal('show');

            // Remove the created HTML from the DOM
            modalElement.on('hidden.bs.modal', function (evt) {
                modalElement.empty();
            });
        });
    };
})
.controller('DeleteNoteController', function ($scope, $rootScope, $state, $stateParams, ResourceUtility, NavUtility, NotesService) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.deleteNoteClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        deleteNote($scope.id, $scope.noteId);
    };

    function deleteNote(id, noteId) {
        $scope.hasDeleteError = false;

        NotesService.DeleteNote(id, noteId).then(function(result) {
            if (result != null && result.success === true) {
                $("#modalContainer").modal('hide');
                
                if ($stateParams.rating_id) {
                    $state.go('home.ratingsengine.dashboard.notes', {}, { reload: true } );
                } else {
                    $state.go('home.model.notes', {}, { reload: true } );
                }
                
            } else {
                $scope.deleteNoteErrorMessage = result.ResultErrors;
                $scope.hasDeleteError = true;
            }
        });
    }

    $scope.cancelClick = function () {
        $("#modalContainer").modal('hide');
    };

});
