(function(){
    var stompClient = null;
    var reconnecting = false;
    var connected = false;
    var authToken = null;

    function setConnected(val) {
        connected = val;
        $("#connect").prop("disabled", val);
        $("#disconnect").prop("disabled", !val);
        if (val) {
            $("#conversation").show();
        }
        else {
            $("#conversation").hide();
        }
    }

    function connect() {
        console.log('Connecting webscoket');
        var socket = new SockJS('https://localhost:9081/pls/sockjs-endpoint');
        stompClient = Stomp.over(socket);
        stompClient.connect({}, function (frame) {
            setConnected(true);
            reconnecting = false;
            showMessage("Connected.");
            console.log('Connected: ' + frame);

            if ( authToken !== null ) {
                authenticate()
            }

            var tenantId = $("#tenantId").val();
            stompClient.subscribe('/topic/' + tenantId+ '/greetings', function (greeting) {
                showMessage(JSON.parse(greeting.body).content);
            });
        }, function(e) {
            if (!reconnecting) {
                connected = false;
                reconnecting = true;
                showMessage(e);
                showMessage("Reconnecting ...");
                if (stompClient !== null) {
                    stompClient.disconnect();
                }
            }
            if (!connected) {
                connect();
            }
        });
    }

    function disconnect() {
        if (stompClient !== null) {
            stompClient.disconnect();
        }
        setConnected(false);
        authToken = null;
        $("#messages").html("");
        console.log("Disconnected.");
    }

    function authenticate() {
        authToken = $("#token").val();
        console.log('Sending token ' + authToken + ' to authenticate the webscoket session.');
        stompClient.send("/app/authenticate", {}, JSON.stringify({'Token': authToken}));
    }

    function showMessage(message) {
        $("#messages").append("<tr><td>" + message + "</td></tr>");
    }

    $(function () {
        $("form").on('submit', function (e) {
            e.preventDefault();
        });
        $( "#connect" ).click(function() { connect(); });
        $( "#disconnect" ).click(function() { disconnect(); });
        $( "#send" ).click(function() { authenticate(); });
    })

}).call(this);

