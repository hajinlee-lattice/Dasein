export default function () {
    return {
        restrict: 'A',
        scope: {
            vm: '='
        },
        templateUrl: '/components/datacloud/explorer/companyprofile/companyprofile.component.html',
        controllerAs: 'vm',
        controller: function ($scope) {
            'ngInject';

            var vm = $scope.vm;

            angular.extend(vm, {
                company_info: $scope.vm.LookupResponse.companyInfo
            });

            vm.format = function (type, value) {
                if (!vm.company_info) {
                    return false;
                }

                var value = value || '',
                    info = vm.company_info;

                switch (type) {
                    case 'address':
                        var address = [];

                        if (info.LDC_Street) {
                            address.push(info.LDC_Street);
                        }

                        if (info.LDC_City) {
                            address.push(info.LDC_City);
                        }

                        if (info.LDC_State) {
                            address.push(info.LDC_State);
                        }

                        if (info.LDC_ZipCode) {
                            address.push(info.LDC_ZipCode.substr(0, 5) + ',');
                        }

                        if (info.LE_COUNTRY) {
                            address.push(info.LE_COUNTRY);
                        }

                        return address.join(' ');

                    case 'phone':
                        if (info.LE_COMPANY_PHONE) {
                            var phone = info.LE_COMPANY_PHONE;
                            return phone.replace(/\D+/g, '').replace(/(\d{3})(\d{3})(\d{4})/, '($1) $2-$3');
                        }

                        break;

                    case 'range':
                        if (value) {
                            var range = value;
                            range = range.replace('-', ' - ');
                            return range;
                        }

                        break;
                }
            }
        }
    };
};