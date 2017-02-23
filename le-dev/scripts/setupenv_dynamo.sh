DYNAMO_ARTIFACT_DIR=$WSHOME/le-dev/dynamo/artifacts
if [ -f $DYNAMO_ARTIFACT_DIR/dynamodb_local_latest.tar.gz ]; then
    echo "Skipping download of Dynamo"
else
    echo "Downloading Dynamo"
    pushd $DYNAMO_ARTIFACT_DIR
    wget http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest.tar.gz
    popd
fi

if [ -d $DYNAMO_HOME ]; then
    echo "Removing old installation directory"
    rm -rf $DYNAMO_HOME
fi

mkdir -p $DYNAMO_HOME
pushd $DYNAMO_HOME
echo "Installing DynamoDB to $DYNAMO_HOME"
tar xzf $DYNAMO_ARTIFACT_DIR/dynamodb_local_latest.tar.gz