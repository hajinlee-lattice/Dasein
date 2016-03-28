use oauth2_dev;

drop table if exists `oauth_client_details`;
create table oauth_client_details (
  client_id varchar(256) primary key,
  resource_ids varchar(256),
  client_secret varchar(256),
  scope varchar(256),
  authorized_grant_types varchar(256),
  web_server_redirect_uri varchar(256),
  authorities varchar(256),
  access_token_validity integer,
  refresh_token_validity integer,
  additional_information varchar(4096),
  autoapprove tinyint
);

drop table if exists `oauth_client_token`;
create table oauth_client_token (
  token_id varchar(256),
  token blob,
  authentication_id varchar(256),
  user_name varchar(256),
  client_id varchar(256)
);

drop table if exists `oauth_access_token`;
create table oauth_access_token (
  token_id varchar(256),
  token blob,
  authentication_id varchar(256),
  user_name varchar(256),
  client_id varchar(256),
  authentication blob,
  refresh_token varchar(256)
);

drop table if exists `oauth_refresh_token`;
create table oauth_refresh_token (
  token_id varchar(256),
  token blob,
  authentication blob
);

drop table if exists `oauth_code`;
create table oauth_code (
  code varchar(256), authentication blob
);

drop table if exists `authorities`;
create table authorities (
  username varchar(256),
  authority varchar(256)
);

drop table if exists `tenant`;
create table tenant (
    pid int not null auto_increment,
    tenant_name varchar(256) not null,
    external_id varchar(256) null,
    jdbc_driver varchar(256) not null,
    jdbc_url varchar(256) not null,
    jdbc_username varchar(256) null,
    jdbc_password varchar(256) null,
    primary key(pid)
);

drop table if exists `oauthuser`;
create table oauthuser (
    pid int not null auto_increment,
    userid varchar(256) not null,
    encryptedpassword varchar(256) not null,
    passwordexpired bit not null,
    passwordexpiration datetime null,
    primary key(pid)
);

insert into oauth_client_details
           (client_id
           ,resource_ids
           ,client_secret
           ,scope
           ,authorized_grant_types
           ,web_server_redirect_uri
           ,authorities
           ,access_token_validity
           ,refresh_token_validity
           ,additional_information
           ,autoapprove)
     values
           ('playmaker', 'playmaker_api', null, 'read,write', 'password,refresh_token', null,
            'role_playmaker_admin', null, null, null, 0);

 insert into oauth_client_details
           (client_id
           ,resource_ids
           ,client_secret
           ,scope
           ,authorized_grant_types
           ,web_server_redirect_uri
           ,authorities
           ,access_token_validity
           ,refresh_token_validity
           ,additional_information
           ,autoapprove)
     values
           ('lp', 'lp_api', null, 'read,write', 'password,refresh_token', null,
            'role_lp_admin', null, null, null, 0);
