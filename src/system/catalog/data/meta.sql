
--
--  BEGIN_COPYRIGHT
--
--  This file is part of SciDB.
--  Copyright (C) 2008-2011 SciDB, Inc.
--
--  SciDB is free software: you can redistribute it and/or modify
--  it under the terms of the GNU General Public License as published by
--  the Free Software Foundation version 3 of the License, or
--  (at your option) any later version.
--
--  SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
--  INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
--  NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
--  the GNU General Public License for the complete license terms.
--
--  You should have received a copy of the GNU General Public License
--  along with SciDB.  If not, see <http://www.gnu.org/licenses/>.
--
-- END_COPYRIGHT
--
-- CLEAR
--
drop table if exists "array" cascade;
drop table if exists "array_version" cascade;
drop table if exists "array_version_lock" cascade;
drop table if exists "array_partition" cascade;
drop table if exists "node" cascade;
drop table if exists "array_attribute" cascade;
drop table if exists "array_dimension" cascade;
drop table if exists "cluster" cascade;
drop table if exists "libraries" cascade;

drop sequence if exists "array_id_seq" cascade;
drop sequence if exists "partition_id_seq" cascade;
drop sequence if exists "node_id_seq" cascade;
drop sequence if exists "libraries_id_seq" cascade;

drop function if exists uuid_generate_v1();
drop function if exists get_cluster_uuid();

--
-- CREATE
--
create sequence "array_id_seq";
create sequence "partition_id_seq";
create sequence "node_id_seq" minvalue 0 start with 0;
create sequence "libraries_id_seq";

create table "cluster"
(
  cluster_uuid uuid
);

create table "array"
(
  id bigint primary key default nextval('array_id_seq'),
  name varchar unique,
  partitioning_schema integer,
  flags integer,
  comment varchar
);

create table "array_version"
(
   array_id bigint references "array" (id) on delete cascade,
   version_id bigint,
   version_array_id bigint references "array" (id) on delete cascade,
   time_stamp bigint,
   primary key(array_id, time_stamp, version_id),
   unique(array_id, version_id)
);

create table "array_version_lock"
(
   array_name varchar,
   array_id bigint,
   query_id bigint,
   node_id bigint,
   array_version_id bigint,
   array_version bigint,
   node_role integer, -- 0-invalid, 1-coordinator, 2-worker
   lock_mode integer, -- {0=invalid, read, write, remove, renameto, renamefrom}
   unique(array_name, query_id, node_id)
);

create table "node"
(
  node_id bigint primary key default nextval('node_id_seq'),
  host varchar,
  port integer,
  online_since timestamp
);

create table "array_partition"
(
  partition_id bigint primary key default nextval('partition_id_seq'),
  array_id bigint references "array" (id) on delete cascade,
  partition_function varchar,
  node_id integer references "node" (node_id) on delete cascade
);

create table "array_attribute"
(
  array_id bigint references "array" (id) on delete cascade,
  id int,
  name varchar not null,
  type varchar,
  flags int,
  default_compression_method int,
  reserve int,
  default_missing_reason int not null,
  default_value varchar null,
  comment varchar,
  primary key(array_id, id),
  unique ( array_id, name )
);

create table "array_dimension"
(
  array_id bigint references "array" (id) on delete cascade,
  id int,
  name varchar not null,
  startMin bigint,
  currStart bigint,
  currEnd bigint,
  endMax bigint,
--
-- PGB: Changed from.
-- start bigint,  - this is currMin
-- length bigint, - this is currMax - currMin

  chunk_interval int,
  chunk_overlap int,
  type varchar,
  source_array_name varchar,
  comment varchar,
  primary key(array_id, id),
  unique ( array_id, name )
);
--
-- Triggers to ensure that, for a given array, the list of the names of the
-- array's attributes and dimensions is unique. That is, that attribute
-- names are not repeated, that dimension names are not repeated, and
-- that no array has an attrinute name that is also a dimension name.
--
create or replace function check_no_array_dupes()
returns trigger as $$
begin
        if (exists ( select 1 from array_dimension AD, array_attribute AA where AD.array_id = AA.array_id AND AD.name = AA.name)) then
                raise exception 'Repeated attribute / dimension name in array %', (select A.name FROM "array" A where A.id = NEW.array_id);
        end if;
        return NEW;
end;
$$language plpgsql;
--
--  One trigger each for the two tables, although both triggers call the
--  same stored procedure.
--
create trigger check_array_repeated_attr_dim_names
after insert or update on array_dimension
for each row execute procedure check_no_array_dupes();

create trigger check_array_repeated_dim_attr_names
after insert or update on array_attribute
for each row execute procedure check_no_array_dupes();

create table "libraries"
(
  id bigint primary key default nextval('libraries_id_seq'),
  name varchar unique
);

create or replace function uuid_generate_v1()
returns uuid
as '$libdir/uuid-ossp', 'uuid_generate_v1'
volatile strict language C;

insert into "cluster" values (uuid_generate_v1());

create function get_cluster_uuid() returns uuid as $$
declare t uuid;
begin
  select into t cluster_uuid from "cluster" limit 1;
  return t;
end;
$$ language plpgsql;

commit;

