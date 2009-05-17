drop sequence branches_id_seq cascade;
drop table branches cascade;
drop table files cascade;
drop table attributes cascade;
drop table names cascade;
drop table ranges cascade;

create sequence branches_id_seq;
create table branches (
	id integer primary key default nextval('branches_id_seq'),
	parent integer not null default currval('branches_id_seq')
		references branches deferrable initially deferred,
	created timestamp not null default now(),
	name text -- A branch may have a name
);
create index branches_name_index on branches (name);

create table files (
	id serial primary key,
	ctime timestamp not null default now()
);

create table attributes (
	id serial primary key,
	branch integer references branches on delete cascade,
	created timestamp default now(),
	file integer references files on delete cascade,
	
	mode integer not null,
	uid integer not null,
	gid integer not null,
	size integer not null default 0,
	atime timestamp not null default now(),
	mtime timestamp not null default now()
);

create table names (
	id serial,
	branch integer not null references branches on delete cascade,
	dir integer not null references files on delete restrict,
	name text not null,
	created timestamp not null default now(),
	deleted timestamp,
	file integer not null references files on delete cascade,
	primary key (id),
	unique(branch, dir, name, created)
);

create table ranges (
	id serial,
	file integer references files on delete cascade,
	branch integer references branches on delete cascade,
	created timestamp default now(),
	start integer not null,
	data bytea not null,
	primary key (id)
);
create index ranges_start_index on ranges (start);
create index ranges_end_index on ranges ((start + octet_length(data)));


begin;
	insert into branches default values;
	
	insert into files default values;
	insert into attributes (branch, file, mode, uid, gid)
		values (currval('branches_id_seq'), currval('files_id_seq'),
			16384 | 420, 0, 0);
	
	insert into names (dir, name, branch, file) values
		(currval('files_id_seq'),
		'.',
		currval('branches_id_seq'),
		currval('files_id_seq'));
	insert into names (dir, name, branch, file) values
		(currval('files_id_seq'),
		'..',
		currval('branches_id_seq'),
		currval('files_id_seq'));
commit;

create language plpgsql;

-- Create a branch - as of now
create or replace function mkbranch(
	p_parent integer,
	out r_new_branch integer)
as $PROC$
declare
begin
	insert into branches (parent) values (p_parent);
	r_new_branch := currval('branches_id_seq');
end;
$PROC$ language plpgsql;

create or replace function mkbranch(
	p_parent integer,
	p_name text,
	out r_new_branch integer)
as $PROC$
declare
begin
	insert into branches (parent, name) values (p_parent, p_name);
	r_new_branch := currval('branches_id_seq');
end;
$PROC$ language plpgsql;

-- Create a new branch as of a given time
create or replace function mkbranch(
	p_parent integer,
	p_created timestamp,
	out r_new_branch integer)
as $PROC$
declare
begin
	insert into branches (parent, created) values (p_parent, p_created);
	r_new_branch := currval('branches_id_seq');
end;
$PROC$ language plpgsql;

create or replace function mkbranch(
	p_parent integer,
	p_created timestamp,
	p_name text,
	out r_new_branch integer)
as $PROC$
declare
begin
	insert into branches (parent, created, name)
		values (p_parent, p_created, p_name);
	r_new_branch := currval('branches_id_seq');
end;
$PROC$ language plpgsql;

-- Get size of a file
create or replace function getsize(
	p_branch integer,
	p_file integer,
	out r_size integer)
as $PROC$
declare
	attribute record;
	current_branch integer;
	
	time_now timestamp;
begin
	found := false;
	current_branch := p_branch;
	time_now := now();
	while not found loop
		select into attribute size from attributes
			where branch = current_branch and
				file = p_file and
				created <= time_now
			order by created desc, id desc limit 1;
		
		if not found then
			select into attribute parent, created from branches
				where id = current_branch;
			if not found or current_branch = attribute.parent then
				found := false;
				exit;
			end if;
			current_branch := attribute.parent;
			time_now := timestamp_smaller(time_now, attribute.created);
			found := false;
		end if;
	end loop;
	
	if not found then
		raise exception 'ENOENT';
	end if;
	
	r_size := attribute.size;
end;
$PROC$ language plpgsql;

create or replace function getattr_no_nlink(
	p_branch integer,
	p_file integer,
	out r_mode integer,
	out r_uid integer,
	out r_gid integer,
	out r_size integer,
	out r_nlink integer,
	out r_atime timestamp,
	out r_mtime timestamp,
	out r_ctime timestamp)
as $PROC$
declare
	attribute record;
	current_branch integer;
	child_branches integer[];
	
	time_now timestamp;
	time_start timestamp;
begin
	found := false;
	time_start := now();
	current_branch := p_branch;
	time_now := time_start;
	while not found loop
		select into attribute mode, uid, gid, size, atime, mtime
			from attributes
			where branch = current_branch and
				file = p_file and
				created <= time_now
			order by created desc, id desc limit 1;
		
		if not found then
			select into attribute parent, created from branches
				where id = current_branch;
			if not found or current_branch = attribute.parent then
				found := false;
				exit;
			end if;
			current_branch := attribute.parent;
			time_now := timestamp_smaller(time_now, attribute.created);
			found := false;
		end if;
	end loop;
	
	if not found then
		-- error not found
		raise exception 'ENOENT';
	end if;
	
	r_mode = attribute.mode;
	r_uid = attribute.uid;
	r_gid = attribute.gid;
	r_size = attribute.size;
	r_atime = attribute.atime;
	r_mtime = attribute.mtime;
	
	select into attribute ctime from files where id = p_file;
	r_ctime := attribute.ctime;
	
	r_nlink := 1;
end;
$PROC$ language plpgsql;

-- Get attributes of a file
create or replace function getattr(
	p_branch integer,
	p_file integer,
	out r_mode integer,
	out r_uid integer,
	out r_gid integer,
	out r_size integer,
	out r_nlink integer,
	out r_atime timestamp,
	out r_mtime timestamp,
	out r_ctime timestamp)
as $PROC$
declare
	attribute record;
	current_branch integer;
	child_branches integer[];
	time_now timestamp;
begin
	select into attribute * from getattr_no_nlink(p_branch, p_file);
	r_mode := attribute.r_mode;
	r_uid := attribute.r_uid;
	r_gid := attribute.r_gid;
	r_size := attribute.r_size;
	r_nlink := attribute.r_nlink;
	r_atime := attribute.r_atime;
	r_mtime := attribute.r_mtime;
	r_ctime := attribute.r_ctime;
	
	-- Count the names for this file
	r_nlink := 0;
	current_branch := p_branch;
	time_now := now();
	loop
		select into attribute count(*) from names n
			where file = p_file and
				branch = current_branch and
				created <= time_now and
				(deleted is null or deleted > time_now) and
				0 = (select count(*) from names where
					file = p_file and
					branch = any(child_branches) and
					created = n.created);
		
		r_nlink := r_nlink + attribute.count;
		
		-- Go up to the parent branch
		perform array_prepend(current_branch, child_branches);
		select into attribute parent, created from branches
			where id = current_branch;
		if not found or current_branch = attribute.parent then
			exit;
		end if;
		current_branch := attribute.parent;
		time_now := timestamp_smaller(time_now, attribute.created);
	end loop;
end;
$PROC$ language plpgsql;

-- Set attributes of a file
create or replace function setattr(
	p_branch integer,
	p_file integer,
	inout p_mode integer,
	inout p_uid integer,
	inout p_gid integer,
	inout p_size integer,
	inout p_atime timestamp,
	inout p_mtime timestamp,
	in p_extend_secure boolean,
	out r_nlink integer,
	out r_ctime timestamp)
as $PROC$
declare
	r_attributes record;
	r_mode integer;
	r_uid integer;
	r_gid integer;
	r_size integer;
	r_atime timestamp;
	r_mtime timestamp;
begin
	select into r_attributes * from getattr(p_branch, p_file);
	r_mode := r_attributes.r_mode;
	r_uid := r_attributes.r_uid;
	r_gid := r_attributes.r_gid;
	r_size := r_attributes.r_size;
	r_nlink := r_attributes.r_nlink;
	r_atime := r_attributes.r_atime;
	r_mtime := r_attributes.r_mtime;
	r_ctime := r_attributes.r_ctime;
	
	if p_mode is null then
		p_mode := r_mode;
	else
		-- Setattr won't change the type of a file
		-- and apparently some NFS clients don't set the type, so mask
		-- that part out to keep what we have already.
		r_mode := r_mode & 61440;
		p_mode := r_mode | (4095 & p_mode);
	end if;
	if p_uid is null then
		p_uid := r_uid;
	end if;
	if p_gid is null then
		p_gid := r_gid;
	end if;
	if p_size is null then
		p_size := r_size;
	else
		-- Must write filling zeroes when extending a file.
		-- Would be better to create empty ranges instead. Would
		-- be more efficient for files with large holes.
		if p_extend_secure and r_size < p_size then
			perform write(p_branch, p_file, r_size,
				decode(repeat('\\000', p_size - r_size), 'escape'), FALSE);
		end if;
	end if;
	if p_atime is null then
		p_atime := r_atime;
	end if;
	if p_mtime is null then
		p_mtime := r_mtime;
	end if;
	
	insert into attributes (branch, file, mode, uid, gid, size, atime, mtime)
		values (p_branch, p_file, p_mode, p_uid, p_gid, p_size, p_atime,
			p_mtime);
end;
$PROC$ language plpgsql;

create or replace function setattr(
	p_branch integer,
	p_file integer,
	inout p_mode integer,
	inout p_uid integer,
	inout p_gid integer,
	inout p_size integer,
	inout p_atime timestamp,
	inout p_mtime timestamp,
	out r_nlink integer,
	out r_ctime timestamp)
as $PROC$
declare
	result record;
begin
	select into result * from setattr(p_branch, p_file, p_mode, p_uid, p_gid,
		p_size, p_atime, p_mtime, TRUE);
	p_mode = result.p_mode;
	p_uid = result.p_uid;
	p_gid = result.p_gid;
	p_size = result.p_size;
	p_atime = result.p_atime;
	p_mtime = result.p_mtime;
	r_nlink = result.r_nlink;
	r_ctime = result.r_ctime;
end;
$PROC$ language plpgsql;

-- Set size of a file
create or replace function setsize(
	p_branch integer,
	p_file integer,
	p_size integer) returns void
as $PROC$
declare
begin
	perform setattr(p_branch, p_file, null, null, null, p_size,
		null, null, FALSE);
end;
$PROC$ language plpgsql;

-- Create a file
create or replace function mkfile(
	p_branch integer,
	p_dir integer,
	p_name text,
	p_mode integer,
	p_uid integer,
	p_gid integer,
	out r_file integer)
as $PROC$
declare
	r_mode integer;
begin
	insert into files default values;
	r_file := currval('files_id_seq');
	perform link(p_branch, r_file, p_dir, p_name);
	
	r_mode := (p_mode & 4095) | 32768;
	
	-- Initially set attr.
	insert into attributes (branch, file, mode, uid, gid)
		values (p_branch, r_file, r_mode, p_uid, p_gid);
end;
$PROC$ language plpgsql;

-- Create a symlink
create or replace function symlink(
	p_branch integer,
	p_dir integer,
	p_name text,
	p_to bytea,
	p_mode integer,
	p_uid integer,
	p_gid integer,
	out r_file integer)
as $PROC$
declare
	r_mode integer;
begin
	insert into files default values;
	r_file := currval('files_id_seq');
	perform link(p_branch, r_file, p_dir, p_name);
	
	r_mode := (p_mode & 4095) | 40960;
	
	-- Initially set attr.
	insert into attributes (branch, file, mode, uid, gid)
		values (p_branch, r_file, r_mode, p_uid, p_gid);
	
	-- Write to the symlink
	perform write(p_branch, r_file, 0, p_to);
end;
$PROC$ language plpgsql;

-- Create a directory
create or replace function mkdir(
	p_branch integer,
	p_dir integer,
	p_name text,
	p_mode integer,
	p_uid integer,
	p_gid integer,
	out r_file integer)
as $PROC$
declare
	r_mode integer;
begin
	insert into files default values;
	r_file := currval('files_id_seq');
	perform link(p_branch, r_file, p_dir, p_name);
	perform link(p_branch, r_file, r_file, '.');
	perform link(p_branch, p_dir, r_file, '..');
	
	r_mode := (p_mode & 4095) | 16384;
	
	insert into attributes(branch, file, mode, uid, gid)
		values (p_branch, r_file, r_mode, p_uid, p_gid);
end;
$PROC$ language plpgsql;

-- Remove a directory
create or replace function rmdir(
	p_branch integer,
	p_dir integer,
	p_name text) returns void
as $PROC$
declare
	r_file integer;
	r record;
begin
	r_file := lookup(p_branch, p_dir, p_name);
	
	-- check for directory not empty.	
	select into r count(*) as count from readdir(p_branch, r_file);
	if r.count = 2 then
		perform unlink_dir(p_branch, r_file, '.');
		perform unlink_dir(p_branch, r_file, '..');
		perform unlink_dir(p_branch, p_dir, p_name);
	else
		raise exception 'ENOTEMPTY';
	end if;
end;
$PROC$ language plpgsql;

-- Lookup a file by name
create or replace function lookup(
	p_branch integer,
	p_dir integer,
	p_name text,
	out r_file integer)
as $PROC$
declare
	name_record record;
	current_branch integer;
	time_now timestamp;
begin
	current_branch := p_branch;
	time_now := now();
	
	-- Find the name
	loop
		select into name_record file from names
			where branch = current_branch and
				dir = p_dir and
				name = p_name and
				created <= time_now and
				(deleted is null or deleted > time_now)
			order by created desc limit 1;
		if found then
			r_file := name_record.file;
			exit;
		else
			select into name_record parent, created from branches
				where id = current_branch;
			if current_branch = name_record.parent then
				-- error - name not found
				raise exception 'ENOENT';
			end if;
			current_branch := name_record.parent;
			time_now := timestamp_smaller(time_now, name_record.created);
		end if;
	end loop;
end;
$PROC$ language plpgsql;

-- Remove a name
create or replace function unlink_dir(
	p_branch integer,
	p_dir integer,
	p_name text) returns void
as $PROC$
declare
begin
	perform unlink(p_branch, p_dir, p_name, TRUE);
end;
$PROC$ language plpgsql;

create or replace function unlink(
	p_branch integer,
	p_dir integer,
	p_name text) returns void
as $PROC$
declare
begin
	perform unlink(p_branch, p_dir, p_name, FALSE);
end;
$PROC$ language plpgsql;

create or replace function unlink(
	p_branch integer,
	p_dir integer,
	p_name text,
	p_is_dir boolean) returns void
as $PROC$
declare
	attributes record;
	name_record record;
	current_branch integer;
	time_now timestamp;
	r_file integer;
	r_mode integer;
	r_uid integer;
	r_gid integer;
	r_size integer;
	r_nlink integer;
	r_atime timestamp;
	r_mtime timestamp;
	r_ctime timestamp;
begin
	current_branch := p_branch;
	time_now := now();
	
	-- Lookup the name
	r_file := lookup(p_branch, p_dir, p_name);
	select into attributes * from getattr_no_nlink(p_branch, r_file);
	r_mode := attributes.r_mode;
	r_uid := attributes.r_uid;
	r_gid := attributes.r_gid;
	r_size := attributes.r_size;
	r_nlink := attributes.r_nlink;
	r_atime := attributes.r_atime;
	r_mtime := attributes.r_mtime;
	r_ctime := attributes.r_ctime;
	
	if p_is_dir then
		if (r_mode & 61440) != 16384 then
			raise exception 'ENOTDIR';
		end if;
	else
		if (r_mode & 61440) = 16384 then
			raise exception 'EISDIR';
		end if;
	end if;
	
	-- Find the name
	loop
		select into name_record * from names
			where branch = current_branch and
				dir = p_dir and
				name = p_name and
				created <= time_now and
				(deleted is null or deleted > time_now)
			order by created desc limit 1;
		if found then
			if current_branch != p_branch then
				insert into names (branch, dir, name, created, deleted, file)
					values (p_branch, p_dir, p_name, name_record.created,
						now(), name_record.file);
			else
				update names set deleted = now() where id = name_record.id;
			end if;
			exit;
		else
			select into name_record parent, created from branches
				where id = current_branch;
			if current_branch = name_record.parent then
				raise exception 'ENOENT';
			end if;
			current_branch := name_record.parent;
			time_now := timestamp_smaller(time_now, name_record.created);
		end if;
	end loop;
end;
$PROC$ language plpgsql;

-- Create a hard link to a file
create or replace function link(
	p_branch integer,
	p_file integer,
	p_dir integer,
	p_name text) returns void
as $PROC$
declare
	r_file integer;
begin
	begin
		r_file := lookup(p_branch, p_dir, p_name);
	exception
		when raise_exception then
			if SQLERRM = 'ENOENT' then
				insert into names (dir, name, branch, file) values
					(p_dir, p_name, p_branch, p_file);
				return;
			end if;
	end;
	
	raise exception 'EEXIST';
end;
$PROC$ language plpgsql;

-- Rename a file - equivalent to a link followed by an unlink
create or replace function rename(
	p_branch integer,
	p_from_dir integer,
	p_from_name text,
	p_to_dir integer,
	p_to_name text) returns void
as $PROC$
declare
	p_from_file integer;
begin
	p_from_file := lookup(p_branch, p_from_dir, p_from_name);
	perform link(p_branch, p_from_file, p_to_dir, p_to_name);
	perform unlink(p_branch, p_from_dir, p_from_name);
end;
$PROC$ language plpgsql;

-- Write to a file
create or replace function write(
	p_branch integer,
	p_file integer,
	p_start integer,
	p_data bytea,
	p_resize boolean) returns void
as $PROC$
declare
	file_size integer;
	overlap record;
	p_end integer;
	set_data bytea;
	set_start integer;
	start_pos integer;
	end_pos integer;
begin
	p_end := p_start + octet_length(p_data);
	
	if p_resize then
		file_size := getsize(p_branch, p_file);
		if p_end > file_size then
			perform setsize(p_branch, p_file, p_end);
		end if;
	end if;
	
	-- Check if there's an existing range that is contiguous with or overlaps
	-- the new range
	select into overlap id, start, data from ranges where
		file = p_file and
		branch = p_branch and
		created = now() and
		(   -- start of range is in our range
			(start >= p_start and start < p_end) or
			-- end of range is in our range
			(start + octet_length(data) >= p_start and
				start + octet_length(data) < p_end));
	
	if found then
		if overlap.start < p_start then
			set_start := overlap.start;
			start_pos := p_start - overlap.start;
			end_pos := start_pos + octet_length(p_data);
			set_data := substring(overlap.data for start_pos) || p_data ||
				substring(overlap.data from end_pos+1);
		else
			set_start := p_start;
			start_pos := overlap.start - p_start;
			end_pos := start_pos + octet_length(overlap.data);
			set_data := substring(p_data for start_pos) || overlap.data ||
				substring(p_data from end_pos+1);
		end if;
		
		update ranges set
			start = set_start, data = set_data
			where id = overlap.id;
	else
		-- If there's no overlapping range, then just add a new range
		insert into ranges (file, branch, start, data) values
			(p_file, p_branch, p_start, p_data);
	end if;
end;
$PROC$ language plpgsql;

create or replace function write(
	p_branch integer,
	p_file integer,
	p_start integer,
	p_data bytea) returns void
as $PROC$
declare
begin
	perform write(p_branch, p_file, p_start, p_data, TRUE);
end;
$PROC$ language plpgsql;

-- Read from a file
create or replace function read(
	p_branch integer,
	p_file integer,
	p_start integer,
	p_length integer,
	out result bytea)
as $PROC$
declare
	file_size integer;
	range record;
	current_start integer;
	current_length integer;
	current_branch integer;
	limit_length integer;
	
	c_from integer;
	c_for integer;
	
	time_now timestamp;
	time_start timestamp;
begin
	time_start := now();
	
	file_size := getsize(p_branch, p_file);
	
	current_start := p_start;
	current_length := int4smaller(p_length, file_size - p_start);
	result := ''::bytea;
	
	if current_length < 0 then
		-- TODO error EOF ?
		-- for now just return empty string
		current_length := 0;
	end if;
	
	while current_length > 0 loop
		current_branch := p_branch;
		limit_length := current_length;
		time_now := time_start;
		
		-- Loop through the current branch, and if necessary, its parent(s).
		loop
			-- Constrain the length to before the most recent range
			-- within the read range.
			select into range min(start) from ranges where
				file = p_file and
				branch = current_branch and
				created <= time_now and
				start > current_start;
			if range.min is not null then
				limit_length := int4smaller(limit_length,
					range.min - current_start);
			end if;
			
			select into range start, data from ranges where
				file = p_file and
				branch = current_branch and
				created <= time_now and
				start <= current_start and
				start + octet_length(data) > current_start
				order by created desc limit 1;
			
			if found then
				exit;
			else
				select into range parent, created
					from branches where id = current_branch;
				if not found or range.parent = current_branch then
					found := false;
					exit;
				else
					current_branch := range.parent;
					time_now := timestamp_smaller(time_now, range.created);
				end if;
			end if;
		end loop;
		if not found then
			result := result ||
				decode(repeat('\\000', limit_length), 'escape');
			current_length := current_length - limit_length;
			current_start := current_start + limit_length;
			continue;
		end if;
		
		c_from := current_start - range.start;
		c_for := int4smaller(octet_length(range.data) - c_from, limit_length);
		
		result := result || substring(range.data from c_from+1 for c_for);
		
		current_length := current_length - c_for;
		current_start := current_start + c_for;
	end loop;
end;
$PROC$ language plpgsql;

-- Read from a directory
drop type direntry cascade;
create type direntry as (
	name text
);
create or replace function readdir(
	p_branch integer,
	p_dir integer) returns setof direntry
as $PROC$
declare
	current_branch integer;
	time_now timestamp;
	name_record record;
	return_val direntry;
begin
	current_branch := p_branch;
	time_now := now();
	
	loop
		for name_record in select name from names where
			branch = current_branch and
			dir = p_dir and
			created <= time_now and
			(deleted is null or deleted > time_now)
		loop
			-- Since it's a setof, it'll deduplicate automagically.
			return_val.name = name_record.name;
			return next return_val;
		end loop;
		
		select into name_record parent, created from branches
			where id = current_branch;
		if current_branch = name_record.parent then
			-- got through the root branch, we are done
			exit;
		end if;
		current_branch := name_record.parent;
		time_now := timestamp_smaller(time_now, name_record.created);
	end loop;
end;
$PROC$ language plpgsql;
