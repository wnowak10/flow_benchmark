var app = angular.module('update_instance_info.module', []);

app.controller('FoobarController', function($scope) {

    var updateDatasets = function() {
        // the parameter to callPythonDo() is passed to the do() method as the payload
        // the return value of the do() method comes back as the data parameter of the fist function()
        $scope.callPythonDo({"datasets": "datasets"}).then(function(data) {
            // success
            $scope.projects = data.projects;
        }, function(data) {
            // failure
            $scope.projects = [];
        });
    };
    updateProjects();
    $scope.$watch('config.allProjects', updateProjects);
    
    var updateFolders = function() {
        // the parameter to callPythonDo() is passed to the do() method as the payload
        // the return value of the do() method comes back as the data parameter of the fist function()
        $scope.callPythonDo({"funtastic": "folders"}).then(function(data) {
            // success
            $scope.folders = data.folders;
        }, function(data) {
            // failure
            $scope.folders = [];
        });
    };
    updateFolders();
    $scope.$watch('config.logFolder', updateFolders);
});


