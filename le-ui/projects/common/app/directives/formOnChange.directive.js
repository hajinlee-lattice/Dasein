angular.module('mainApp.appCommon.directives.formOnChange', [])
.directive('formOnChange', function($parse, $interpolate){
  return {
    require: "form",
    link: function(scope, element, attrs, form){
      var callback = $parse(attrs.formOnChange);
      element.on("change", function(){
        callback(scope);
      });
    }
  };
});