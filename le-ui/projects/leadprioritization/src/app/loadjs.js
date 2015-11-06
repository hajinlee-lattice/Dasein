var JsLoader = function(){

    this.toLinkedList = function(pairs) {
        if (pairs.length < 1) return null;

        var tail = {};
        var head = tail;

        for (var i = 0; i < pairs.length; i++) {
            tail.path = pairs[i].path;
            tail.fallback = pairs[i].fallback;
            tail.next = {};
            tail = tail.next;
        }

        return head;
    };

    this.loadScriptByObj = loadScriptByObj;

    function loadScriptByObj(obj) {
        var callback = function(){ };

        if (obj.hasOwnProperty("next") && obj.next !== null &&
            obj.next.hasOwnProperty("path") && obj.next.hasOwnProperty("fallback")) {
            callback = function(){ loadScriptByObj(obj.next); };
        }

        var async = obj.async || false;
        loadScript(obj.path, obj.fallback, callback, async);
    }

    function loadScript(path, fallback, callback, async) {
        var done = false;
        var js = document.createElement('script');
        js.onload = handleLoad;
        js.onreadystatechange = handleReadyStateChange;
        js.onerror = handleError;
        js.src = path;
        js.async = async;
        js.type = 'text/javascript';
        var s = document.getElementsByTagName('script')[0];
        s.parentNode.insertBefore(js, s);

        function handleLoad() {
            if (!done) {
                done = true;
                callback();
            }
        }

        function handleReadyStateChange() {
            var state;

            if (!done) {
                state = js.readyState;
                if (state === "complete")
                    handleLoad();
            }
        }

        function handleError() {
            if (!done) {
                done = true;
                console.log("Failed to load resource at " + path);
                if (fallback !== null) {
                    loadScript(fallback, null, callback, async);
                } else {
                    callback();
                }
            }
        }
    }
};


