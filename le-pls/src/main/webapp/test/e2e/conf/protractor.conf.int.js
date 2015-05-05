'use strict';

var common = require('./protractor.conf.common');

var config = common.generateConfig('..');

config.params.tenantName = "IntegrationTest Tenant 2";
config.params.alternativeTenantName = "IntegrationTest Tenant 1";

exports.config = config;