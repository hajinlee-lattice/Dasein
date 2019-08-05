/* jshint node: true */
/* jshint esnext: true */
/* jshint sub:true */
/* jshint -W030 */
"use strict";

/*
                Lattice Engines Express Server
    See Gruntfile.js to define LOCAL environment variables
    See /conf/env for setting SERVER environment variables
*/

const path = require("path");
const exphbs = require("express-handlebars");
const request = require("request");
const morgan = require("morgan");
const fs = require("graceful-fs");
const helmet = require("helmet");
const compress = require("compression");
const chalk = require("chalk");
const cors = require("cors");
const wsproxy = require("http-proxy-middleware");
const DateUtil = require("./utilities/DateUtil");
// const session = require("express-session");
const bodyParser = require('body-parser');
var TrayRouter = require('./apis/tray/tray_routes');
var ResetPasswordRouter = require('./apis/reset/reset_password_router');
var ZendeskRouter = require('./apis/zendesk/zendesk_routes');
class Server {
    constructor(express, app, options) {
        console.log(
            chalk.white(">") + " SETTINGS",
            JSON.stringify(options, null, 4)
        );
        console.log("\n");

        this.options = options;
        this.express = express;
        this.app = app;
        //this.setCors();

        if (options.config.protocols.http) {
            const http = require("http");

            http.globalAgent.maxSockets = Infinity;
            this.httpServer = http.createServer(this.app);
        }

        if (options.config.protocols.https) {
            try {
                const tls = require("tls");
                const constants = require("constants");
                const https = require("https");

                const httpsKey = fs.readFileSync(
                    options.config.HTTPS_KEY,
                    "utf8"
                );
                const httpsCert = fs.readFileSync(
                    options.config.HTTPS_CRT,
                    "utf8"
                );

                const tlsOptions = {
                    cert: httpsCert,
                    secureOptions:
                        constants.SSL_OP_NO_SSLv2 | constants.SSL_OP_NO_SSLv3
                };

                options.config.HTTPS_KEY ? (tlsOptions.key = httpsKey) : null;
                options.config.HTTPS_PASS
                    ? (tlsOptions.passphrase = options.config.HTTPS_PASS)
                    : null;

                tls.CLIENT_RENEG_LIMIT = 0;
                https.globalAgent.maxSockets = Infinity;

                this.httpsServer = https.createServer(tlsOptions, this.app);
            } catch (err) {
                console.log(
                    chalk.red(DateUtil.getTimeStamp() + ":httpserror>"),
                    err
                );
            }
        }

        // order matters
        this.setRenderer();
        this.setMiddleware();
        options.config.WHITELIST
            ? this.trustProxy(options.config.WHITELIST)
            : null;
        options.config.LOGGING
            ? this.startLogging(options.config.LOGGING)
            : null;
        options.routes ? this.setAppRoutes(options.routes) : null;
        this.setProxies();
        this.setDefaultRoutes(options.config.NODE_ENV);
        // this.setOtherRoutes(options.config.NODE_ENV);
    }

    setCors() {
        this.app.use(cors());
        this.app.options("*", cors());
    }

    setRenderer() {
        // set up view engine for handlebars
        this.app.engine(".html", exphbs({ extname: ".html" }));
        this.app.set("view engine", ".html");
        this.app.set("views", this.options.config.APP_ROOT);
    }

    setMiddleware() {
        // enable gzip compression
        this.app.use(compress());

        // helmet enables/modifies/removes http headers for security concerns
        // x-frame-options: sameorigin gets set, can be removed with config allow_from
        this.app.use(helmet({ frameguard: false }));

        // default cookie behavior - httpOnly for googles approval
        // note: i dont think we have any apps that use cookies :D
        // this.app.use(
        //     session({
        //         //name: 'sessionId',
        //         secret: "LEs3Cur1ty",
        //         resave: false,
        //         saveUnitialized: false,
        //         cookie: {
        //             httpOnly: true,
        //             secure: true,
        //             expires: new Date(Date.now() + 60 * 60 * 1000) // 1 hour
        //         }
        //     })
        // );
    }

    startLogging(log_path) {
        let logDirectory = log_path;

        try {
            const accessLogStream = fs.createWriteStream(
                logDirectory + "/" + this.options.name + "_access.log",
                {
                    flags: "a"
                }
            );

            const map = {
                default: "dev",
                verbose: "combined"
            };

            morgan.token("datetime", function getDateTime() {
                return DateUtil.getTimeStamp();
            });

            morgan.token("utctime", function getDateTime() {
                return new Date().getTime();
            });

            this.app.use(
                morgan(map[this.options.config.LOG_LEVEL], {
                    stream: accessLogStream
                })
            );

            this.app.use(
                morgan("dev", {
                    skip: function (req, res) {
                        return res.statusCode < 400;
                    }
                })
            );
        } catch (err) {
            console.log(chalk.red(DateUtil.getTimeStamp() + ":logging>") + err);
        }
    }

    // trust the load balancer/proxy in production
    trustProxy(WHITELIST) {
        this.app.set("trust proxy", ip => {
            const ips = WHITELIST.split(",");

            ips.forEach(current_ip => {
                if (current_ip === ip) {
                    return true;
                }
            });

            return false;
        });
    }

    setProxies() {
        const options = this.options;
        console.log('=============================================');
        console.log('SET PROXIES ', options.config.proxies);
        console.log('=============================================');
        for (let path in options.config.proxies) {
            const proxy = options.config.proxies[path];
            switch (proxy.type) {
                case "websocket":
                    this.createWsProxy(
                        proxy.remote_host,
                        proxy.local_path,
                        proxy.remote_path
                    );
                    break;
                case "file_pipe":
                    this.createFileProxy(
                        proxy.remote_host,
                        proxy.local_path,
                        proxy.remote_path
                    );
                    break;
                case "sse_pipe":
                    this.createSSEProxy(
                        proxy.remote_host,
                        proxy.local_path,
                        proxy.remote_path
                    );
                    break;
                case "pipe":
                    this.createApiProxy(
                        proxy.remote_host,
                        proxy.local_path,
                        proxy.remote_path
                    );
                    break;
                case 'tray_pipe':
                    this.createTrayProxy(
                        proxy.remote_host,
                        proxy.local_path,
                        proxy.remote_path
                    );
                    break;
                case 'zdsk_pipe':
                    this.createZendeskProxy(
                        proxy.remote_host,
                        proxy.local_path,
                        proxy.remote_path
                    );
                    break;
                    case 'reset_pipe':
                    this.createResetPasswordRouter(
                        proxy.remote_host,
                        proxy.local_path,
                        proxy.remote_path
                    );
                    break;
                default:
                    // throw error invalid configuration?
                    break;
            }
        }
    }

    // forward Websocket requests for dev
    createWsProxy(API_URL, API_LOCAL_PATH, API_PATH) {
        // check if internal IP

        if (API_URL) {
            API_PATH = API_PATH || "/pls";
            const remoteUrl = API_URL + API_PATH;
            console.log(
                chalk.white(">") + " WS PROXY\t",
                API_LOCAL_PATH,
                "\n\t" + remoteUrl + "\n"
            );
            try {
                const websocketProxy = wsproxy(API_LOCAL_PATH, {
                    target: API_URL,
                    changeOrigin: true,
                    ws: true,
                    secure: false,
                    proxyTimeout: 60000
                });
                this.app.use(websocketProxy);
            } catch (err) {
                console.log(
                    chalk.red(DateUtil.getTimeStamp() + ":wsproxy>") + err
                );
            }
        }
    }

    // forward API requests for dev
    createApiProxy(API_URL, API_LOCAL_PATH, API_PATH) {
        // check if internal IP

        if (API_URL) {
            API_PATH = API_PATH || "/pls";

            console.log(
                chalk.white(">") + " API PROXY\t",
                API_LOCAL_PATH,
                "\n\t" + API_URL + API_PATH + "\n"
            );

            try {
                this.app.use(API_LOCAL_PATH, (req, res) => {
                    const reqUrl =
                        req.url[1] === "?" ? req.url.substring(1) : req.url;
                    const url = API_URL + API_PATH + reqUrl;
                    let r = null;

                    try {
                        // set default api proxy timeout to be 10 min
                        res.setTimeout(600000);
                        if (req.method === "POST") {
                            r = request.post({
                                uri: url,
                                json: req.body
                            });

                            // prevent large-ish files from timing out on upload
                            res.setTimeout(1800000);
                        } else {
                            r = request(url);
                        }

                        req.pipe(r).pipe(res);
                    } catch (err) {
                        console.log(
                            chalk.red(DateUtil.getTimeStamp() + ":apiproxy>") +
                            err
                        );
                    }
                });
            } catch (err) {
                console.log(
                    chalk.red(DateUtil.getTimeStamp() + ":apiroute>") + err
                );
            }
        }
    }

    // this is needed so that the browser can download files
    // that need to be hidden behind the Authorization token
    createFileProxy(API_URL, API_PATH, PATH) {
        if (API_URL) {
            (API_PATH = API_PATH || "/files"), (PATH = PATH || "/pls");

            console.log(
                chalk.white(">") + " FILE PROXY",
                API_PATH,
                "\n\t" + API_URL + PATH
            );

            this.app.use(API_PATH, (req, res) => {
                // urls heading to /files/* will go to /pls/* with Auth token
                const url = API_URL + PATH + req.url;

                try {
                    let r = request(url);

                    if (req.query) {
                        if (req.query.Authorization) {
                            // Since the token was in the URL, +'s got converted to spaces
                            let token = req.query.Authorization.replace(
                                / /g,
                                "+"
                            );
                            req.headers["Authorization"] = token || "";
                        }

                        if (req.query.TenantId) {
                            let tenant = req.query.TenantId;
                            req.headers["TenantId"] = tenant || "";
                        }
                    }

                    req.pipe(r).pipe(res);
                } catch (err) {
                    console.log(
                        chalk.red(DateUtil.getTimeStamp() + ":fileproxy>") + err
                    );
                }
            });
        }
    }

    // this is needed to establish server sent event connections
    createSSEProxy(API_URL, API_PATH, PATH) {
        if (API_URL) {
            (API_PATH = API_PATH || "/sse"), (PATH = PATH || "/pls");

            console.log(
                chalk.white(">") + " SSE PROXY",
                API_PATH,
                "\n\t" + API_URL + PATH
            );

            this.app.use(API_PATH, (req, res) => {
                // urls heading to /sse/* will go to /pls/* with Auth token
                const url = API_URL + PATH + req.url;

                try {
                    let r = request(url);

                    if (req.query) {
                        if (req.query.Authorization) {
                            // Since the token was in the URL, +'s got converted to spaces
                            let token = req.query.Authorization.replace(
                                / /g,
                                "+"
                            );
                            req.headers["Authorization"] = token || "";
                        }

                        if (req.query.TenantId) {
                            let tenant = req.query.TenantId;
                            req.headers["TenantId"] = tenant || "";
                        }

                        if (req.query.Method) {
                            let method = req.query.Method;
                            req.method = method || "GET";
                        }
                    }

                    req.pipe(r).pipe(res);
                } catch (err) {
                    console.log(
                        chalk.red(DateUtil.getTimeStamp() + ":sseproxy>") + err
                    );
                }
            });
        }
    }

    createTrayProxy(API_URL, API_PATH, PATH) {
        if(API_URL){
            console.log('TRAY PROXY <======================');
            var router = new TrayRouter(this.express, this.app, bodyParser, chalk,  API_URL, PATH, request, this.options.config.proxies, this.options.config.TRAY_MASTER_AUTHORIZATION).createRoutes();
            this.app.use('/tray', router);
        }
    }

    createResetPasswordRouter(API_URL, API_PATH, PATH){
        if(API_URL){
            console.log('Reset Password PROXY <======================');
            var router = new ResetPasswordRouter(this.express, this.app, bodyParser, chalk,  API_URL, PATH, request, this.options.config.proxies).createRoutes();
            this.app.use('/reset', router);
        }
    }

    createZendeskProxy(API_URL, API_PATH, PATH) {
        if (API_URL) {
            console.log('Zendesk Password PROXY <======================');
            var router = new ZendeskRouter(this.express, this.app, bodyParser, chalk,  API_URL, PATH, request, this.options.config.proxies).createRoutes();
            this.app.use('/zdsk', router);
        }
    }

    setAppRoutes(routes) {
        //this.app.use('/', (req, res) => res.redirect(301, '/login'));
        routes.forEach(route => {
            const dir = this.options.config.APP_ROOT + route.path;
            var displayString = "";

            /*
             * Redirect the user to a different url
             */
            if (route.redirect) {
                Object.keys(route.redirect).forEach(redirect => {
                    this.app.all(redirect, (req, res) =>
                        res.redirect(route.redirect[redirect])
                    );
                });
            }

            /*
             * Static routes for app files
             */
            if (route.folders) {
                Object.keys(route.folders).forEach(folder => {
                    displayString +=
                        (displayString ? ", " : "") + route.folders[folder];
                    this.app.use(
                        folder,
                        this.express.static(dir + route.folders[folder]) //, { maxAge: 3600000 })
                    );
                });
            }

            displayString = "";

            /*
             * Webapp entry points
             */
            if (route.pages) {
                const html5mode = route.html5mode === true;

                Object.keys(route.pages).forEach(page => {
                    //console.log('\t'+route.pages[page]+'\t->',page);
                    // if !internal ip && page == '/admin'
                    this.app.get(page + (html5mode ? "*" : ""), (req, res) => {
                        /*
                            let host = req.get('host');
                            let origin = req.get('origin');
                            let s = host.split('.');
                            var domain = s.length > 1 ? [ s.pop(), s.pop() ].reverse().join('.') : s[0];

                            console.log(
                                'PAGE[' + page + ']' +
                                ', XFRAME-OPTIONS-HOST: ' + domain +' | '+ host +
                                ', ORIGIN: ' + origin +' | '+ req.headers.origin +
                                ', ALLOWED: ' + (route.xframe_allow && route.xframe_allow.indexOf(domain) > -1)
                            );

                            if (route.xframe_allow) {
                                if (route.xframe_allow.indexOf(domain) > -1) {
                                   res.removeHeader('X-Frame-Options');
                                }
                            }
                            */
                           if (route.xframe_options) {
                               res.setHeader('X-Frame-Options', route.xframe_options);
                           }

                        res.render(dir + "/" + route.pages[page]);
                    });
                });
            }
        });
    }

    setDefaultRoutes(NODE_ENV) {
        // catch 404 and forwarding to error handler
        this.app.use((req, res, next) => {
            const err = new Error("Not Found");
            err.status = 404;
            next(err);
        });

        // print stack trace for dev environment
        if (NODE_ENV != "production" && NODE_ENV != "qa") {
            this.app.use((err, req, res, next) => {
                res.status(err.status || 500);
                res.render("server/error", {
                    options: JSON.stringify(this.options, null, "\t"),
                    url: req.originalUrl,
                    status: err.status,
                    message: err.message,
                    error: err,
                    env: NODE_ENV
                });
            });
        }

        // no stacktraces leaked to user for production
        this.app.use((err, req, res, next) => {
            res.status(err.status || 500);
            res.render("server/error", {
                url: req.originalUrl,
                status: err.status,
                message: err.message,
                error:
                    "Please contact the administrator if the problem persists.",
                env: false
            });
        });
    }
    // setOtherRoutes(NODE_ENV) {
    //     var router = new TrayRouter(this.express).createRoutes();
    //     this.app.use('/tray', router);
    //     // this.otherRoutes(this.app, {});
    // }

    start(cb) {
        const options = this.options;
        const config = options.config;

        //if (options.NODE_ENV == 'development') {
        //    console.log(chalk.yellow('> TLS:') + ' Allow Unauthorized in Development Mode')
        process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = "0";
        //}

        console.log("\n");
        if (this.httpServer) {
            this.httpServer.listen(config.protocols.http, err => {
                console.log(
					chalk.red(config.TIMESTAMP + ">") +
						" LISTENING: http://localhost:" +
						config.protocols.http
				);
                console.log(
					chalk.white.bgRed(
						"If you are developing an IFRAME with https protocol use the https protocol"
					)
				);
                cb(err, {
                    proto: "http",
                    port: config.protocols.http,
                    app: options.name
                });
            });
        }

        if (this.httpsServer) {
            this.httpsServer.listen(config.protocols.https, err => {
                console.log(
                    chalk.green(config.TIMESTAMP + ">") +
                    " LISTENING: https://localhost:" +
                    config.protocols.https
                );
                console.log("\n");

                cb(err, {
                    proto: "https",
                    port: config.protocols.https,
                    app: options.name
                });
            });
        }
    }
}
module.exports = Server;
