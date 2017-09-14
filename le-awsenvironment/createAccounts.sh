#requirement:  yum install jq
# key assumption - script is run on formation of EC@ instance, it does not assume it maintains very well.  If the user exists, but no ssh key is in  the accounts. the sudo and authorized keys files is removed.  But if a user is removed from the group, we do not know to remove the account fomr the system.

#aws iam list-ssh-public-keys --user-name mpechner
#iam get-ssh-public-key --user-name mpechner --ssh-public-key-id APKAJZD2ZZWARC6HGYCA  --encoding SSH

# The one parameter is a valid AWS IAM group name
if [ "$#" -ne 1 ];then
   echo "Need the name of the Group"
   exit 1
fi

Groupname=$1

#Gets the list of ssh keys for a user - users can have multiple SSH keys.  Almost always used because we are rotating keys.
function getSSHIDS {
    user=$1
    userids=`aws iam list-ssh-public-keys --user-name $user | jq .SSHPublicKeys[]`
    if [[ $? -ne 0 ]] ; then
        echo "No SSH keys"
       userKeyList=''
    else 
       userKeyList=(`echo $userids| jq .SSHPublicKeyId | tr -d '"' `)
    fi
}

#Get A ssh key
function getPubSSH {
    user=$1
    key=$2
    keyobj=`aws iam get-ssh-public-key --user-name $user  --ssh-public-key-id $key  --encoding SSH`
    if [[ $? -ne 0 ]] ; then
        keystr=''
    else
	    keystr=`echo $keyobj | jq .SSHPublicKey.SSHPublicKeyBody | tr -d '"'`
	    if [[ ${#keystr} -lt 100 ]] ; then
		keystr=''
	    fi
    fi
}



# query froup for information

groupinfo=`aws iam get-group --group-name $Groupname`
if [ $? -ne 0 ] ; then
    echo "FAILED: probably invalid group name."
    exit 1
fi

#grab the list of users from the json
userlist=`echo $groupinfo | jq .Users[].UserName | tr -d '"'`
if [[ ${#userlist[@]} -le 0 ]] ; then
    echo "Empty user list"
    exit -1
fi

for user in $userlist
do
    
    echo "User:$user"
    
    getSSHIDS $user
    if [[ ${#userKeyList[@]} -le 0 ]] ; then
        echo "$user does not have a SSH Key"
	rm -f /home/${user}/.ssh/authorized_keys /etc/sudoers.d/${user}
        continue
    fi
    
    if [ ! -d  /home/$user ] ; then
	    useradd -m $user
    fi
    
    if [ ! -d  /home/${user}/.ssh ] ; then
	    mkdir /home/${user}/.ssh
	    chown ${user} /home/${user}/.ssh
	    chgrp  ${user} /home/${user}/.ssh
	    chmod 700   /home/${user}/.ssh
    fi

    if [ ! -e /home/${user}/.ssh/authorized_keys ] ; then
	    touch  ${user} /home/${user}/.ssh/authorized_keys
	    chown ${user} /home/${user}/.ssh/authorized_keys
	    chgrp  ${user} /home/${user}/.ssh/authorized_keys
	    chmod 700   /home/${user}/.ssh/authorized_keys
	    touch  ${user} /home/${user}/.ssh/authorized_keys
    fi  

    # for each key id, get ssh key 
    for keyid in ${userKeyList[@]}
    do
        getPubSSH $user $keyid
        if [ -n "$keystr" ] ; then
		echo $keystr >> /home/${user}/.ssh/authorized_keys
 		#yes should only run once
		echo "${user}  ALL = NOPASSWD: ALL" > /etc/sudoers.d/${user}
	else
		echo "WARNING: $user does not have a valid ssh public key"
	fi
    done

done


