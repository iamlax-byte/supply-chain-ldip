-- =============================================================================
-- 00_create_schemas.sql
-- Creates all pipeline schemas and grants the ldip_user access to each.
-- Executed automatically by MySQL Docker on first container start.
-- =============================================================================

create schema if not exists `raw`;
create schema if not exists staging;
create schema if not exists warehouse;
create schema if not exists marts;
create schema if not exists metadata;

-- Grant ldip_user (created via MYSQL_USER env var) access to all schemas.
-- 'raw' access is already granted by Docker via MYSQL_DATABASE; the rest are explicit.
grant all privileges on `raw`.*       to 'ldip_user'@'%';
grant all privileges on staging.*     to 'ldip_user'@'%';
grant all privileges on warehouse.*   to 'ldip_user'@'%';
grant all privileges on marts.*       to 'ldip_user'@'%';
grant all privileges on metadata.*    to 'ldip_user'@'%';
flush privileges;
