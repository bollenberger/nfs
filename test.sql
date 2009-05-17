drop table myfiles cascade;
drop table myrefs;

create table myfiles (id integer primary key);

create table myrefs (myid integer primary key, id integer references myfiles deferrable initially deferred);

create rule myrule as on delete to myrefs
	where 1=(select count(*) from myrefs where id = OLD.id)
do also (
	delete from myfiles where id = OLD.id
);

insert into myfiles values (0);
insert into myfiles values (1);

insert into myrefs values (0, 0);
insert into myrefs values (1, 0);
insert into myrefs values (2, 0);
insert into myrefs values (3, 1);
insert into myrefs values (4, 1);

delete from myrefs where myid=3;
select * from myfiles;
delete from myrefs where myid=4;
select * from myfiles;
