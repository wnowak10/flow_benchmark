var app = angular.module('update_instance_info.module', []);

app.controller('FoobarController', function($scope) {

    var updateDatasets = function() {
        // the parameter to callPythonDo() is passed to the do() method as the payload
        // the return value of the do() method comes back as the data parameter of the fist function()
        $scope.callPythonDo({"funtastic": "Datasets"}).then(function(data) {
            // success
            $scope.Datasets = data.Datasets;
        }, function(data) {
            // failure
            $scope.Datasets = [];
        });
    };
    updateDatasets();
    $scope.$watch('config.allDatasets', updateDatasets);
    
    
});


