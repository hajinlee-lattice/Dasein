"use strict";

module.exports = {
    protocols: {
        http: parseInt(process.env.HTTP_PORT) || undefined,
        https: parseInt(process.env.HTTPS_PORT) || undefined
    },
    proxies: {
        "/pls/ws": {
            local_path: "/pls/ws",
            remote_host: process.env.API_URL || "http://localhost:8081",
            remote_path: "/pls/ws",
            type: "websocket"
        },
        "/pls": {
            local_path: "/pls",
            remote_host: process.env.API_URL || "http://localhost:8081",
            remote_path: "/pls",
            type: "pipe"
        },
        "/pls_alt": {
            local_path: "/pls",
            remote_host: process.env.API_URL || "http://localhost:9081",
            remote_path: "/pls",
            type: "pipe"
        },
        "/ulysses": {
            local_path: "/ulysses",
            remote_host: process.env.ULYSSES_URL || "http://localhost:8075",
            remote_path: "/ulysses",
            type: "pipe"
        },
        "/DanteService.svc": {
            local_path: "/dante",
            remote_host: process.env.DANTE_URL || "http://localhost:8081",
            remote_path: "/",
            type: "pipe"
        },
        "/score": {
            local_path: "/score",
            remote_host: process.env.API_CON_URL || "http://localhost:8073",
            remote_path: "/score",
            type: "pipe"
        },
        "/files": {
            local_path: "/files",
            remote_host: process.env.API_URL || "http://localhost:8080",
            remote_path: "/pls",
            type: "file_pipe"
        },
        "/sse": {
            local_path: "/sse",
            remote_host: process.env.API_URL || "http://localhost:8081",
            remote_path: "/pls",
            type: "sse_pipe"
        },
        "/tray": {
            local_path: "/tray",
            remote_host: process.env.TRAY_API_URL || "http://localhost:8081",
            remote_path: "",
            type: "tray_pipe"
        },
        "/zdsk": {
            local_path: "/zdsk",
            remote_host: process.env.API_URL || "http://localhost:8081",
            remote_path: "",
            type: "zdsk_pipe"
        },
        "/reset": {
            local_path: "/reset",
            remote_host: process.env.API_URL || "http://localhost:8081",
            remote_path: "",
            type: "reset_pipe"
        }
    }
};
