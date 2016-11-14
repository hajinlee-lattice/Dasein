"use strict";

module.exports = function (grunt) {
    // version of our software. This should really be in the package.json
    // but it gets passed in through 
    var versionStringConfig = grunt.option('versionString') || new Date().getTime();

    // Define the configuration for all the tasks
    grunt.initConfig({
        versionString: versionStringConfig,
        dir: {
            common: './projects/common',
            lp: './projects/leadprioritization',
            pd: './projects/prospectdiscovery',
            login: './projects/login',
            assets: 'assets',
            app: 'app'
        },

        // https://confluence.lattice-engines.com/pages/viewpage.action?pageId=16909888
        env: {
            dev: {
                NODE_APPS: 'leui',
                NODE_ENV: 'development',
                API_URL: 'https://testapp.lattice-engines.com',
                //API_URL: 'https://10.41.0.13:8081',
                API_ADMIN_URL: 'https://10.41.0.25:8085',
                API_CON_URL: 'https://testapi.lattice-engines.com:8073',
                API_MCSVC_URL: 'https://10.41.0.25:8080',
                API_MATCHAPI_URL: 'https://10.41.0.25:8076',
                API_INFLUXDB_URL: 'http://10.41.1.188:8086',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false
            },
            dev_admin: {
                NODE_APPS: 'leadmin',
                NODE_ENV: 'development',
                API_URL: 'https://testapp.lattice-engines.com',
                //API_URL: 'https://10.41.0.13:8081',
                API_ADMIN_URL: 'https://10.41.0.25:8085',
                API_CON_URL: 'https://testapi.lattice-engines.com:8073',
                API_MCSVC_URL: 'https://10.41.0.25:8080',
                API_MATCHAPI_URL: 'https://10.41.0.25:8076',
                API_INFLUXDB_URL: 'http://10.41.1.188:8086',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false
            },
            devb: {
                NODE_APPS: 'leui,leadmin',
                NODE_ENV: 'development',
                API_URL: 'https://bodcdevsvipb13.lattice.local:8081',
                API_ADMIN_URL: 'https://10.41.0.26:8085',
                API_CON_URL: 'https://bodcdevsvipb26.lattice.local:8073',
                API_MCSVC_URL: 'https://10.41.0.26:8080',
                API_MATCHAPI_URL: 'https://10.41.0.26:8076',
                API_INFLUXDB_URL: 'http://10.41.1.188:8086',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false
            },
            devb_admin: {
                NODE_APPS: 'leadmin',
                NODE_ENV: 'development',
                API_ADMIN_URL: 'https://10.41.0.26:8085',
                API_MCSVC_URL: 'https://10.41.0.26:8080',
                API_MATCHAPI_URL: 'https://10.41.0.26:8076',
                API_INFLUXDB_URL: 'http://10.41.1.188:8086',
                COMPRESSED: false,
                LOGGING: './server/log',
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false
            },
            local: {
                NODE_APPS: 'leui',
                NODE_ENV: 'development',
                API_URL: 'http://localhost:8081',
                API_ADMIN_URL: 'http://localhost:8085',
                API_CON_URL: 'http://localhost:8073',
                API_MCSVC_URL: 'http://localhost:8080',
                API_MATCHAPI_URL: 'http://localhost:8076',
                API_INFLUXDB_URL: 'http://localhost:8086',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false
            },
            local_admin: {
                NODE_APPS: 'leadmin',
                NODE_ENV: 'development',
                API_URL: 'http://localhost:8081',
                API_ADMIN_URL: 'http://localhost:8085',
                API_CON_URL: 'http://localhost:8073',
                API_MCSVC_URL: 'http://localhost:8080',
                API_MATCHAPI_URL: 'http://localhost:8076',
                API_INFLUXDB_URL: 'http://localhost:8086',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false
            },
            qa: {
                NODE_APPS: 'leui,leadmin',
                NODE_ENV: 'qa',
                API_URL: 'https://testapp.lattice-engines.com',
                //API_URL: 'https://bodcdevsvipb13.lattice.local', // for b
                API_ADMIN_URL: 'https://admin-qa.lattice.local:8085',
                API_CON_URL: 'https://testapi.lattice-engines.com',
                API_MCSVC_URL: 'https://admin-qa.lattice.local:8080',
                API_MATCHAPI_URL: 'https://admin-qa.lattice.local:8076',
                API_INFLUXDB_URL: 'http://localhost:8086',
                COMPRESSED: true,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false,
                WHITELIST: '10.41.0.14, 10.41.0.16'
            },
            qadev: {
                NODE_APPS: 'leui,leadmin',
                NODE_ENV: 'qa',
                API_URL: 'https://testapp.lattice-engines.com',
                //API_URL: 'https://bodcdevsvipb13.lattice.local', // for b
                API_ADMIN_URL: 'https://admin-qa.lattice.local:8085',
                API_CON_URL: 'https://testapi.lattice-engines.com',
                API_MCSVC_URL: 'https://admin-qa.lattice.local:8080',
                API_MATCHAPI_URL: 'https://admin-qa.lattice.local:8076',
                API_INFLUXDB_URL: 'http://localhost:8086',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false,
                WHITELIST: '10.41.0.14, 10.41.0.16'
            },
            production: {
                NODE_APPS: 'leui',
                NODE_ENV: 'production',
                API_URL: 'https://app.lattice-engines.com',
                API_ADMIN_URL: ' https://admin.prod.lattice.local:8085/',
                API_CON_URL: 'https://api.lattice-engines.com',
                COMPRESSED: true,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false,
                WHITELIST: '10.51.12.109, 10.51.51.109'
            },
            proddev: {
                NODE_APPS: 'leui',
                NODE_ENV: 'production',
                API_URL: 'https://app.lattice-engines.com',
                API_ADMIN_URL: 'https://admin.prod.lattice.local:8085/',
                API_CON_URL: 'https://api.lattice-engines.com',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false,
                WHITELIST: '10.51.12.109, 10.51.51.109'
            }
        },

        run: {
            node: {
                args: [ './app.js' ]
            },
            nodemon: {
                cmd: 'nodemon',
                args: [ './app.js' ]
            },
            pm2: {
                cmd: 'pm2.cmd',
                args: [ 'start', './app.js' ]
            },
            killnode: {
                cmd: 'taskkill.exe',
                args: [
                    '/F',
                    '/IM',
                    'node.exe'
                ]
            }
        },

        sass: {
            options: {
                sourcemap: 'auto',
                style:     'compressed'
            },
            common: {
                files: {
                    '<%= dir.common %>/<%= dir.assets %>/lattice.css' : [
                        '<%= dir.common %>/<%= dir.assets %>/sass/lattice.scss'
                    ]
                }
            },
            login:     {
                files: {
                    '<%= dir.login %>/<%= dir.assets %>/css/production.css': [
                        '<%= dir.login %>/<%= dir.app %>/app.component.scss'
                    ]
                }
            },
            lp:     {
                files: {
                    '<%= dir.lp %>/<%= dir.assets %>/styles/production.css': [
                        '<%= dir.lp %>/<%= dir.assets %>/styles/main.scss'
                    ]
                }
            },
            pd:     {
                files: {
                    '<%= dir.pd %>/<%= dir.assets %>/styles/production.css': [
                        '<%= dir.pd %>/<%= dir.app %>/app.scss'
                    ]
                }
            }
        },

        watch: {
            common: {
                files: '<%= dir.common %>/<%= dir.assets %>/sass/*.scss',
                tasks: ['sass:common']
            },
            login: {
                files: [
                    '<%= dir.login %>/<%= dir.app %>/**/*.scss'
                ],
                tasks: ['sass:login']
            },
            lp: {
                files: [
                    '<%= dir.lp %>/<%= dir.app %>/**/*.scss',
                    '<%= dir.lp %>/<%= dir.assets %>/styles/**/*.scss'
                ],
                tasks: ['sass:lp']
            },
            pd: {
                files: [
                    '<%= dir.pd %>/<%= dir.assets %>/styles/**/*.scss'
                ],
                tasks: ['sass:pd']
            }
        },

        concurrent: {
            sass: {
                tasks: [ 'sass:common', 'sass:login', 'sass:lp' ]
            },
            watch: {
                tasks: [ 'watch:common', 'watch:login', 'watch:lp' ],
                options: {
                    logConcurrentOutput: true
                }
            },
            devWatchAndServe: {
                tasks: [ 
                    [ 'env:dev', 'run:node' ], 'concurrent:watch'
                ],
                options: {
                    logConcurrentOutput: true
                }
            },
            devbWatchAndServe: {
                tasks: [ 
                    [ 'env:devb', 'run:node' ], 'concurrent:watch'
                ],
                options: {
                    logConcurrentOutput: true
                }
            },
            localWatchAndServe: {
                tasks: [ 
                    [ 'env:local', 'run:node' ], 'concurrent:watch'
                ],
                options: {
                    logConcurrentOutput: true
                }
            },
            qaWatchAndServe: {
                tasks: [ 
                    [ 'env:qa', 'run:node' ], 'concurrent:watch'
                ],
                options: {
                    logConcurrentOutput: true
                }
            },
            prodWatchAndServe: {
                tasks: [ 
                    [ 'env:proddev', 'run:node' ], 'concurrent:watch'
                ],
                options: {
                    logConcurrentOutput: true
                }
            }
        }
    });

    grunt.loadNpmTasks('grunt-concurrent');
    grunt.loadNpmTasks('grunt-contrib-sass');
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-run');
    grunt.loadNpmTasks('grunt-env');

    grunt.registerTask('default', [
        'env:production',
        'run:node'
    ]);

    grunt.registerTask('dev', [
        'env:dev',
        'run:node'
    ]);

    grunt.registerTask('devb', [
        'env:devb',
        'run:node'
    ]);

    grunt.registerTask('qa', [
        'env:qa',
        'run:node'
    ]);

    grunt.registerTask('prod', [
        'env:production',
        'run:node'
    ]);

    grunt.registerTask('newdev', [
        'concurrent:sass',
        'concurrent:devWatchAndServe'
    ]);

    grunt.registerTask('newdevb', [
        'concurrent:sass',
        'concurrent:devbWatchAndServe'
    ]);

    grunt.registerTask('newlocal', [
        'concurrent:sass',
        'concurrent:localWatchAndServe'
    ]);

    grunt.registerTask('qadev', [
        'concurrent:qaWatchAndServe'
    ]);

    grunt.registerTask('proddev', [
        'concurrent:prodWatchAndServe'
    ]);

    grunt.registerTask('production', [
        'env:production',
        'run:node'
    ]);

    grunt.registerTask('dev_admin', [
        'env:dev_admin',
        'run:node'
    ]);

    grunt.registerTask('devb_admin', [
        'env:devb_admin',
        'run:node'
    ]);

    grunt.registerTask('local_admin', [
        'env:local_admin',
        'run:node'
    ]);

    grunt.registerTask('local', [
        'env:local',
        'run:node'
    ]);

    grunt.registerTask('localmon', [
        'env:local',
        'run:nodemon'
    ]);

    grunt.registerTask('qa', [
        'env:qa',
        'run:node'
    ]);

    grunt.registerTask('qamon', [
        'env:qa',
        'run:nodemon'
    ]);

    grunt.registerTask('devmon', [
        'env:dev',
        'run:nodemon'
    ]);

    grunt.registerTask('killnode', [
        'run:killnode'
    ]);
};