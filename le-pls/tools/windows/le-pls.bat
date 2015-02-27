@echo off

set cmd=%1

IF /i [%cd:~-6%] NEQ [le-pls] (
    goto eof
)

FOR /f "tokens=3 delims=><" %%A IN ('type pom.xml ^| find "certificate.name"') DO (
    SET certificate=%%A & goto endLoop
)
:endLoop

if "%cmd%"=="compile" (

    echo compiling using shared GlobalAuth ...

    mvn -Pfunctional -Pgenerate ^
        -Djavax.net.ssl.trustStore=certificates\%certificate% ^
        -DPLS_PROPDIR=conf\env\dev-windows ^
        clean compile

) else if "%cmd%"=="verify" (

    echo running clean verify using shared GlobalAuth ...

    mvn -Pfunctional -Pgenerate ^
        -Djavax.net.ssl.trustStore=certificates\%certificate% ^
        -DPLS_PROPDIR=conf\env\dev-windows ^
        -DargLine="" ^
        clean verify

) else if "%cmd%"=="popdata" (

    echo populate testing data using shared GlobalAuth ...

    mvn -Pfunctional -Pgenerate ^
        -Djavax.net.ssl.trustStore=certificates\%certificate% ^
        -DPLS_PROPDIR=conf\env\dev-windows ^
        -DargLine="" ^
        -Dtest=*ModelSummaryResourceTestNG

) else if "%cmd%"=="run" (

    echo running clean verify using shared GlobalAuth ...

    mvn -Pfunctional ^
        -Djavax.net.ssl.trustStore=certificates\%certificate% ^
        -DPLS_PROPDIR=conf\env\dev-windows ^
        jetty:run

) else if "%cmd%"=="compile-local" (

    echo compiling using localhost GlobalAuth ...

    mvn -Pfunctional -Pgenerate ^
        -Djavax.net.ssl.trustStore=certificates\localhost.jks ^
        -DPLS_PROPDIR=conf\env\dev-windows-localGA ^
        -Dcertificate.name=localhost.jks ^
        -Dglobalauth.base.url=https://localhost/Bard ^
        clean compile

) else if "%cmd%"=="verify-local" (

    echo running clean verify using localhost GlobalAuth ...

     mvn -Pfunctional -Pgenerate ^
        -Djavax.net.ssl.trustStore=certificates\localhost.jks ^
        -DPLS_PROPDIR=conf\env\dev-windows-localGA ^
        -Dcertificate.name=localhost.jks ^
        -Dglobalauth.base.url=https://localhost/Bard ^
        -DargLine="" ^
        clean verify

) else if "%cmd%"=="popdata-local" (

    echo populate testing data using localhost GlobalAuth ...

    mvn -Pfunctional -Pgenerate ^
        -Djavax.net.ssl.trustStore=certificates\localhost.jks ^
        -DPLS_PROPDIR=conf\env\dev-windows-localGA ^
        -Dcertificate.name=localhost.jks ^
        -Dglobalauth.base.url=https://localhost/Bard ^
        -DargLine="" ^
        -Dtest=*ModelSummaryResourceTestNG

) else if "%cmd%"=="run-local" (

    echo running clean verify using localhost GlobalAuth ...

    mvn -Pfunctional ^
        -Djavax.net.ssl.trustStore=certificates\localhost.jks ^
        -DargLine="" ^
        -DPLS_PROPDIR=conf\env\dev-windows-localGA ^
        jetty:run

) else (

    echo unknown command: %cmd%

)

:eof

echo in "ledp/le-pls" directory run "tools\windows\le-pls.bat {command}"
echo currently available {command} are:
echo    1. compile: compile using remote GlobalAuth, a fast way of checking GlobalAuth
echo    2. verify: full functional test, using remote GlobalAuth
echo    3. popdata: populate testing data (faster than "verify"), using remote GlobalAuth
echo    4. run: jetty:run using remote GlobalAuth
echo    5. compile-local, verify-local, popdata-local, run-local: same as above, but using localhost GlobalAuth