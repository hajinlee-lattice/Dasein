use `oauth2DB`;

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
CREATE INDEX client_id_idx ON oauth_client_details (client_id);

drop table if exists `oauth_client_token`;
create table oauth_client_token (
  token_id varchar(256),
  token blob,
  authentication_id varchar(256),
  user_name varchar(256),
  client_id varchar(256)
);
CREATE INDEX token_id_idx ON oauth_client_token(token_id);
CREATE INDEX username_idx ON oauth_client_token (user_name);
CREATE INDEX client_id_idx ON oauth_client_token(client_id);

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
CREATE UNIQUE INDEX token_id_idx ON oauth_access_token(token_id);
CREATE INDEX username_idx ON oauth_access_token(user_name);
CREATE INDEX client_id_idx ON oauth_access_token(client_id);
CREATE INDEX authentication_id_idx ON oauth_access_token(authentication_id);

drop table if exists `oauth_refresh_token`;
create table oauth_refresh_token (
  token_id varchar(256),
  token blob,
  authentication blob
);
CREATE INDEX token_id_idx ON oauth_refresh_token (token_id);

drop table if exists `oauth_code`;
create table oauth_code (
  code varchar(256), authentication blob
);
CREATE INDEX code_idx ON oauth_code (code);

drop table if exists `authorities`;
create table authorities (
  username varchar(256),
  authority varchar(256)
);
CREATE INDEX username_idx ON authorities (username);

drop table if exists `TENANT`;
create table TENANT (
    PID int not null auto_increment,
    TENANT_NAME varchar(256) not null,
    EXTERNAL_ID varchar(256) null,
    JDBC_DRIVER varchar(256) not null,
    JDBC_URL varchar(256) not null,
    JDBC_USERNAME varchar(256) null,
    JDBC_PASSWORD varchar(256) null,
    JDBC_PASSWORD_ENCRYPT varchar(256) null,
    GW_API_KEY varchar(256) null,
    primary key(PID)
);
CREATE UNIQUE INDEX tenant_name_idx ON TENANT (TENANT_NAME);

drop table if exists `OAuthUser`;
create table OAuthUser (
    PID int not null auto_increment,
    UserId varchar(256) not null,
    EncryptedPassword varchar(256) not null,
    PasswordExpired bit not null,
    PasswordExpiration datetime null,
    primary key(PID)
);
CREATE UNIQUE INDEX IX_UserId ON OAuthUser (UserId);

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
