'use strict';

module.exports = function (grunt) {
    // version of our software. This should really be in the package.json
    // but it gets passed in through
    var versionStringConfig = grunt.option('versionString') || new Date().getTime();

    // Define the configuration for all the tasks
    grunt.initConfig({
        versionString: versionStringConfig,
        dir: {
            common: './projects/common',
            atlas: './projects/atlas',
            lpi: './projects/lpi',
            pd: './projects/prospectdiscovery',
            login: './projects/login',
            ng2: './projects/ng2',
            assets: 'assets',
            components: 'components',
            app: 'app'
        },

        // https://confluence.lattice-engines.com/display/ENG/AWS+Stack+Topology
        env: {
            devall: { // all populated, qa stack a
                NODE_APPS: 'leui,leadmin',
                NODE_ENV: 'development',
                DANTE_URL: 'https://bis-awstest.lattice-engines.com',
                API_URL: 'https://internal-public-lpi-a-1482626327.us-east-1.elb.amazonaws.com',
                API_CON_URL: 'https://internal-public-lpi-a-1482626327.us-east-1.elb.amazonaws.com',
                ULYSSES_URL: 'https://internal-public-lpi-a-1482626327.us-east-1.elb.amazonaws.com',
                API_ADMIN_URL: 'https://internal-private-lpi-a-1832171025.us-east-1.elb.amazonaws.com',
                API_MCSVC_URL: 'https://internal-private-lpi-a-1832171025.us-east-1.elb.amazonaws.com',
                API_MATCHAPI_URL: 'https://internal-private-lpi-a-1832171025.us-east-1.elb.amazonaws.com',
                API_INFLUXDB_URL: 'http://internal-influx-1992709958.us-east-1.elb.amazonaws.com:8086',
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
            dev: { // qa stack a
                NODE_APPS: 'leui',
                NODE_ENV: 'development',
                DANTE_URL: 'https://bis-awstest.lattice-engines.com',
                ULYSSES_URL: 'https://internal-public-lpi-a-1482626327.us-east-1.elb.amazonaws.com',
                API_URL: 'https://internal-public-lpi-a-1482626327.us-east-1.elb.amazonaws.com',
                API_CON_URL: 'https://internal-public-lpi-a-1482626327.us-east-1.elb.amazonaws.com',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false,
                LEUI_BUNDLER: 'grunt'
            },
            dev_admin: { // qa stack a
                NODE_APPS: 'leadmin',
                NODE_ENV: 'development',
                API_ADMIN_URL: 'https://internal-private-lpi-a-1832171025.us-east-1.elb.amazonaws.com',
                API_MCSVC_URL: 'https://internal-private-lpi-a-1832171025.us-east-1.elb.amazonaws.com',
                API_MATCHAPI_URL: 'https://internal-private-lpi-a-1832171025.us-east-1.elb.amazonaws.com',
                API_INFLUXDB_URL: 'http://internal-influx-1992709958.us-east-1.elb.amazonaws.com:8086',
                COMPRESSED: false,
                LOGGING: './server/log',
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false,
                LEADMIN_BUNDLER: 'grunt'
            },
            devb: { // qa stack b
                NODE_APPS: 'leui',
                NODE_ENV: 'development',
                DANTE_URL: 'https://bis-awstest.lattice-engines.com',
                ULYSSES_URL: 'https://internal-public-lpi-b-507116299.us-east-1.elb.amazonaws.com',
                API_URL: 'https://internal-public-lpi-b-507116299.us-east-1.elb.amazonaws.com',
                API_CON_URL: 'https://internal-public-lpi-b-507116299.us-east-1.elb.amazonaws.com',
                API_MATCHAPI_URL: 'https://internal-private-lpi-b-282775961.us-east-1.elb.amazonaws.com',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false,
                LEUI_BUNDLER: 'grunt'
            },
            devb_admin: { // qa stack b
                NODE_APPS: 'leadmin',
                NODE_ENV: 'development',
                API_ADMIN_URL: 'https://internal-private-lpi-b-282775961.us-east-1.elb.amazonaws.com',
                API_MCSVC_URL: 'https://internal-private-lpi-b-282775961.us-east-1.elb.amazonaws.com',
                API_MATCHAPI_URL: 'https://internal-private-lpi-b-282775961.us-east-1.elb.amazonaws.com',
                API_INFLUXDB_URL: 'http://internal-influx-1992709958.us-east-1.elb.amazonaws.com:8086',
                COMPRESSED: false,
                LOGGING: './server/log',
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false,
                LEADMIN_BUNDLER: 'grunt'
            },
            devb_bodc: { // qa stack b in bodc, to be deprecated
                NODE_APPS: 'leui',
                NODE_ENV: 'development',
                ULYSSES_URL: 'https://10.41.0.13:8075',
                API_URL: 'https://10.41.0.13:8081',
                API_CON_URL: 'https://10.41.0.13:8073',
                API_MATCHAPI_URL: 'https://10.41.0.26:8076',
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
            devb_admin_bodc: { // qa stack b in bodc, to be deprecated
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
                ULYSSES_URL: 'https://localhost:9075',
                API_URL: 'https://localhost:9081',
                API_CON_URL: 'https://localhost:9073',
                API_ADMIN_URL: 'https://localhost:9085',
                API_MCSVC_URL: 'https://localhost:9080',
                API_MATCHAPI_URL: 'https://localhost:9076',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false,
                LEUI_BUNDLER: 'grunt'
            },
            local2: {
                NODE_APPS: 'leui',
                NODE_ENV: 'development',
                ULYSSES_URL: 'https://localhost:9075',
                API_URL: 'https://localhost:9081',
                API_CON_URL: 'https://localhost:9073',
                API_ADMIN_URL: 'https://localhost:9085',
                API_MCSVC_URL: 'https://localhost:9080',
                API_MATCHAPI_URL: 'https://localhost:9076',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false
            },
            local_admin: {
                NODE_APPS: 'leadmin',
                NODE_ENV: 'development',
                API_ADMIN_URL: 'https://localhost:9085',
                API_MCSVC_URL: 'https://localhost:9080',
                API_MATCHAPI_URL: 'https://localhost:9076',
                API_INFLUXDB_URL: 'https://localhost:9086',
                COMPRESSED: false,
                LOGGING: './server/log',
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false,
                LEADMIN_BUNDLER: 'grunt'
            },
            qa: { // qa stack a
                NODE_APPS: 'leui,leadmin',
                NODE_ENV: 'qa',
                ULYSSES_URL: 'https://internal-public-lpi-a-1482626327.us-east-1.elb.amazonaws.com',
                API_URL: 'https://internal-public-lpi-a-1482626327.us-east-1.elb.amazonaws.com',
                API_ADMIN_URL: 'https://internal-private-lpi-a-1832171025.us-east-1.elb.amazonaws.com',
                API_CON_URL: 'https://internal-public-lpi-a-1482626327.us-east-1.elb.amazonaws.com',
                API_MCSVC_URL: 'https://internal-private-lpi-a-1832171025.us-east-1.elb.amazonaws.com',
                API_MATCHAPI_URL: 'https://internal-private-lpi-a-1832171025.us-east-1.elb.amazonaws.com',
                API_INFLUXDB_URL: 'http://internal-influx-1992709958.us-east-1.elb.amazonaws.com:8086',
                COMPRESSED: true,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false,
                WHITELIST: 'internal-private-lpi-a-1832171025.us-east-1.elb.amazonaws.com, internal-public-lpi-a-1482626327.us-east-1.elb.amazonaws.com',
                LEUI_BUNDLER: 'grunt',
                LEADMIN_BUNDLER: 'grunt'

            },
            qadev: { // qa stack a
                NODE_APPS: 'leui,leadmin',
                NODE_ENV: 'qa',
                ULYSSES_URL: 'https://internal-public-lpi-a-1482626327.us-east-1.elb.amazonaws.com',
                API_URL: 'https://internal-public-lpi-a-1482626327.us-east-1.elb.amazonaws.com',
                API_ADMIN_URL: 'https://internal-private-lpi-a-1832171025.us-east-1.elb.amazonaws.com',
                API_CON_URL: 'https://internal-public-lpi-a-1482626327.us-east-1.elb.amazonaws.com',
                API_MCSVC_URL: 'https://internal-private-lpi-a-1832171025.us-east-1.elb.amazonaws.com',
                API_MATCHAPI_URL: 'https://internal-private-lpi-a-1832171025.us-east-1.elb.amazonaws.com',
                API_INFLUXDB_URL: 'http://internal-influx-1992709958.us-east-1.elb.amazonaws.com:8086',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false,
                WHITELIST: 'internal-private-lpi-a-1832171025.us-east-1.elb.amazonaws.com, internal-public-lpi-a-1482626327.us-east-1.elb.amazonaws.com',
                LEUI_BUNDLER: 'grunt',
                LEADMIN_BUNDLER: 'grunt'
            },
            production: {
                NODE_APPS: 'leui',
                NODE_ENV: 'production',
                ULYSSES_URL: 'https://bodcdevsvipb13.lattice.local:8075',
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
                ULYSSES_URL: 'https://api.lattice-engines.com',
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
            },
            proda: { // prod stack a
                NODE_APPS: 'leui',
                NODE_ENV: 'production',
                ULYSSES_URL: 'https://internal-public-lpi-a-1059974862.us-east-1.elb.amazonaws.com',
                API_URL: 'https://internal-public-lpi-a-1059974862.us-east-1.elb.amazonaws.com',
                API_ADMIN_URL: 'https://internal-private-lpi-a-418154873.us-east-1.elb.amazonaws.com',
                API_CON_URL: 'https://internal-public-lpi-a-1059974862.us-east-1.elb.amazonaws.com',
                COMPRESSED: true,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false,
                WHITELIST: 'internal-public-lpi-a-1059974862.us-east-1.elb.amazonaws.com, internal-private-lpi-a-418154873.us-east-1.elb.amazonaws.com'
            },
            prodb: { // prod stack b
                NODE_APPS: 'leui',
                NODE_ENV: 'production',
                ULYSSES_URL: 'https://internal-public-lpi-b-556082394.us-east-1.elb.amazonaws.com',
                API_URL: 'https://internal-public-lpi-b-556082394.us-east-1.elb.amazonaws.com',
                API_ADMIN_URL: 'https://internal-private-lpi-b-1755219837.us-east-1.elb.amazonaws.com',
                API_CON_URL: 'https://internal-public-lpi-b-556082394.us-east-1.elb.amazonaws.com',
                COMPRESSED: true,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false,
                WHITELIST: 'internal-public-lpi-b-556082394.us-east-1.elb.amazonaws.com, internal-private-lpi-b-1755219837.us-east-1.elb.amazonaws.com'
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
            atlas:     {
                files: {
                    '<%= dir.atlas %>/<%= dir.assets %>/styles/production.css': [
                        '<%= dir.atlas %>/<%= dir.assets %>/styles/main.scss'
                    ]
                }
            },
            lpi:     {
                files: {
                    '<%= dir.lpi %>/<%= dir.assets %>/styles/production.css': [
                        '<%= dir.lpi %>/<%= dir.assets %>/styles/main.scss'
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
                files: [
                    '<%= dir.common %>/<%= dir.assets %>/sass/*.scss',
                    '<%= dir.common %>/<%= dir.components %>/**/*.scss'
                ],
                tasks: ['sass:common']
            },
            login: {
                files: [
                    '<%= dir.login %>/<%= dir.app %>/**/*.scss'
                ],
                tasks: ['sass:login']
            },
            atlas: {
                files: [
                    '<%= dir.atlas %>/<%= dir.app %>/**/*.scss',
                    '<%= dir.atlas %>/<%= dir.assets %>/styles/**/*.scss'
                ],
                tasks: ['sass:atlas']
            },
            lpi: {
                files: [
                    '<%= dir.lpi %>/<%= dir.app %>/**/*.scss',
                    '<%= dir.lpi %>/<%= dir.assets %>/styles/**/*.scss'
                ],
                tasks: ['sass:lpi']
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
                tasks: [ 'sass:common', 'sass:login', 'sass:atlas', 'sass:lpi' ]
            },
            watch: {
                tasks: [ 'watch:common', 'watch:login', 'watch:atlas', 'watch:lpi'/*, 'run:ng2'*/ ],
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
            devbBodcWatchAndServe: {
                tasks: [
                    [ 'env:devb_bodc', 'run:node' ], 'concurrent:watch'
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
            ng2: {
                exec: 'cd ./projects/ng2/ && ng build --watch'
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

        webpack: {
            ng2: {
                entry: '<%= dir.ng2 %>/src/index.html',
                output: {
                    path: '<%= dir.ng2 %>/dist/',
                    filename: 'inline2.js',
                },

                stats: {
                    // Configure the console output (stats: false disables)
                    colors: true,
                    modules: true,
                    reasons: true
                },

                storeStatsTo: 'webpackOutput', // writes the status to a variable named xyz
                // you may use it later in grunt i.e. <%= xyz.hash %>

                progress: true, // Don't show progress

                failOnError: false, // don't report error to grunt if webpack find errors
                // Use this if webpack errors are tolerable and grunt should continue

                watch: true, // use webpacks watcher
                // You need to keep the grunt process alive
                /*
                watchOptions: {
                    aggregateTimeout: 500,
                    poll: true
                },
                */
                // Use this when you need to fallback to poll based watching (webpack 1.9.1+ only)

                keepalive: true, // don't finish the grunt task
                // Use this in combination with the watch option

                inline: false,  // embed the webpack-dev-server runtime into the bundle
                hot: false, // adds the HotModuleReplacementPlugin and switch the server to hot mode
                // Use this in combination with the inline option

            }
        }
    });

    grunt.loadNpmTasks('grunt-concurrent');
    grunt.loadNpmTasks('grunt-contrib-sass');
    grunt.loadNpmTasks('grunt-contrib-watch');
    //grunt.loadNpmTasks('grunt-webpack');
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

    grunt.registerTask('newdevb_bodc', [
        'concurrent:sass',
        'concurrent:devbBodcWatchAndServe'
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

    grunt.registerTask('devallnowatch', [
        'env:devall',
        'run:node'
    ]);

    grunt.registerTask('devallmon', [
        'env:devall',
        'run:nodemon'
    ]);

    grunt.registerTask('local', [
        'env:local',
        'run:node'
    ]);

    grunt.registerTask('local2', [
        'env:local2',
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