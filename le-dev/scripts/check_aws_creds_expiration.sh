function check_aws_creds_expiration() {
  if [[ $(hostname) = *"hlth-slv"* ]]; then
    echo "This is a trunk health node, no need to check aws creds";
  elif [[ -f "${HOME}/.aws/credentials" ]]; then
    expiration=$(cat ${HOME}/.aws/credentials | grep -C 6 'default' | grep "expiration" | cut -d ' ' -f 3- | cut -d '+' -f 1)
    echo "Your aws creds is expiring at ${expiration}"
    if [[ "$(uname)" == "Darwin" ]]; then
        exp_ts=$(TZ="UTC" date -jf "%Y-%m-%d %H:%M:%S" "${expiration}" +%s)
    else
        exp_ts=$(date -u -d "${expiration}" +%s)
    fi
    current_ts=$(date +%s)
    remaining_min=$(( (exp_ts - current_ts) / 60 ))
    if (( remaining_min > 30 )); then
      echo "The current aws creds is still valid for ${remaining_min} minutes, ok to continue using it."
    else
      echo "The current aws creds is only valid for ${remaining_min} minutes, please re-login aws cli!"
      exit 1
    fi
  else
    echo "Cannot find ~/.aws/credentials. Must setup aws cli!"
    exit 1
  fi
}
