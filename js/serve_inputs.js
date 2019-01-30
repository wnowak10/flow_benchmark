var app = angular.module('update_instance_info.module', []);

app.controller('FoobarController', function($scope) {
    var updateChoices = function() {
        // the parameter to callPythonDo() is passed to the do() method as the payload
        // the return value of the do() method comes back as the data parameter of the fist function()
        $scope.callPythonDo({}).then(function(data) {
            // success
            console.log(data);
            $scope.choices = data.choices;
        }, function(data) {
            // failure
            console.log(data);
            $scope.choices = [];
        });
    };
    updateChoices();
    $scope.$watch('config.filterColumn', updateChoices);
});