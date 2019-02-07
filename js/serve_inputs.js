// For documentation, see: https://doc.dataiku.com/dss/latest/plugins/reference/other.html#fetching-data-for-custom-forms

var app = angular.module('update_instance_info.module', []);

app.controller('FoobarController', function($scope) {
    var updateChoices = function() {
        $scope.callPythonDo({"funtastic": "engines"}).then(function(data) {
            // success
            $scope.engines = data.engines;
        }, function(data) {
            // failure
            $scope.engines = [];
        });
    };
    updateChoices();
    $scope.$watch('config.computeEngine', updateChoices);
    
    var updateConnections = function() {
        $scope.callPythonDo({"funtastic": "connections"}).then(function(data) {
            console.log(data)
            console.log(data.connections)
            $scope.connections = data.connections;
        }, function(data) {
            $scope.connections = [];
        });
    };
    updateConnections();
    $scope.$watch('config.connectionType', updateConnections);
    
    var dynamicButton = function() {
        $scope.callPythonDo({"funtastic": "dynamic"}).then(function(data) {
            $scope.dynamic = data.dynamic;
        }, function(data) {
            $scope.dynamic = [];
        });
    };
    dynamicButton();
    $scope.$watch('config.dynamic', dynamicButton);
    
// });
});