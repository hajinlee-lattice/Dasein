angular.module('exceptionOverride', [
])
.factory('$exceptionHandler', function () {
    return function (exception, cause) {
      exception.message += ' (caused by "' + cause + '")';
      if (console != null) {
          console.log("Error: "+ exception.message);
      }
    };
  });