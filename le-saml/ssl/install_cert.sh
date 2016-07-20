#!/bin/bash

if [ $USER != "root" ]; then
    echo "Must be root to run this command"
    exit 1
fi

if [ $# -ne 2 ]; then
    echo "Usage $0 JAVA_HOME WSHOME";
    exit 1
fi

echo "JAVA_HOME: $1"
echo "WSHOME: $2"

KEYALIAS=jetty123
TRUSTED_KEYSTORE=$1/jre/lib/security/cacerts
TRUSTED_STOREPASS=changeit
KEYSTORE=$2/le-saml/target/*.keystore
KEYPASS=jetty123
STOREPASS=jetty123


exists=`keytool -keystore $TRUSTED_KEYSTORE -storepass $TRUSTED_STOREPASS -list | grep $KEYALIAS`

if [ -n "$exists" ]; then
    echo "Deleting existing certificate from trusted store..."
    keytool -delete -keystore $TRUSTED_KEYSTORE -storepass $TRUSTED_STOREPASS -alias $KEYALIAS -noprompt
fi

echo "Exporting generated certificate..."
keytool -exportcert -alias $KEYALIAS -keystore $KEYSTORE -storepass $STOREPASS -noprompt > /tmp/key.cert

echo "Installing certificate to trusted store..."
keytool -import -trustcacerts -keystore $TRUSTED_KEYSTORE -storepass $TRUSTED_STOREPASS -noprompt -alias $KEYALIAS -file /tmp/key.cert


