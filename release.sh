function get_version() {
  cat pom.xml | grep \<version\> | head -n 1 | cut -d \< -f 2 | cut -d \> -f 2
}

function set_version() {
  TARGET_VERSION=$1
  if [ ${TARGET_VERSION} == "" ]; then
    echo "Must provide a valid TARGET_VERSION"
    exit -1
  fi
  CURRENT_VERSION=$(get_version)
  echo "Change version from ${CURRENT_VERSION} to ${TARGET_VERSION}"
  for i in $(grep -rnl "${CURRENT_VERSION}" --include "*.xml" --exclude-dir .git .);
  do
    if [[ $(uname) == 'Darwin' ]]; then
      sed -i '' "s|${CURRENT_VERSION}|${TARGET_VERSION}|g" ${i}
    else
      sed -i "s|${CURRENT_VERSION}|${TARGET_VERSION}|g" ${i}
    fi
  done
}
