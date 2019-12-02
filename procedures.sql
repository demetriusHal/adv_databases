
--- question 1 ---

CREATE or replace FUNCTION function1(time1 timestamp without time zone, time2 timestamp without time zone) 
   RETURNS TABLE(logtypes varchar(32), total bigint)
    LANGUAGE sql
    AS $$ SELECT log.type, COUNT(id) as counts
		FROM log 
		WHERE log.time BETWEEN time1 AND time2
		GROUP BY log.type
		ORDER BY counts DESC
$$;


select function1('01-01-1999', '31-12-2019')

--- question 2 ---


CREATE OR REPLACE FUNCTION function2(t1 timestamp without time zone, t2 timestamp without time zone, logtype varchar(32)) 
   RETURNS TABLE(day date, total bigint)
    LANGUAGE sql
    AS $$
   select cast(time as date),count(*)
   from log
   where time between t1 and  t2 and log.type = logtype
   group by cast(time as date)
$$;


select function2('01-01-1999', '31-12-2019', 'replicate')


--- question 3 ---

CREATE OR REPLACE FUNCTION function3(d date)
	returns table(source_ip varchar(32), logtype varchar(32), count bigint)
	  LANGUAGE sql
AS $$
with 
grouped as(
   select source_ip, type, count(*) as counts
   from log
	where  time between   d and  d+interval'24 hours'
   group by source_ip, type

),
rowed as (
select source_ip, type, counts, row_number() over (
   partition by source_ip
   order by counts
   ) as rownum
   from grouped
)
select source_ip, type, counts
from rowed
where rownum = 1
$$

select function3('2016-01-29' )




---question 4 ---

create or replace function function4(time1 timestamp, time2 timestamp)
	   RETURNS TABLE(blockid varchar(32), total bigint)
    LANGUAGE sql
    AS $$
select block_requested, count(*)
from blocks,log
where log_id=log.id and log.time between time1 and time2
group by block_requested
order by count(*)
desc
limit 5
$$

select function4('01-01-1999', '31-12-2019')


--- question 5 ---

create or replace function function5()
	   RETURNS TABLE(referer varchar )
    LANGUAGE sql
    AS $$
select referer
from access
group by referer
having count(distinct resource) > 1
$$

--- question 6 ---

create or replace function function6()
	   RETURNS TABLE(res varchar(256), total bigint)
    LANGUAGE sql
    AS $$
with counted as (
	select resource, count(*) as counts
	from access
	group by resource
	order by counts
	desc
	limit 2
)
select resource, counts
from counted
order by counts
asc
limit 1
$$

--- question 7 ---
create or replace function function7(sz bigint )
	   RETURNS TABLE( 
		     "id" int,
  "time" TIMESTAMP ,
  "source_ip" VARCHAR(32),
  "type" VARCHAR(32) ,
	"user_id" VARCHAR(32) ,
  "http_method" VARCHAR(32) ,
  "resource" VARCHAR(256) ,
  "response" VARCHAR(32) ,
  "response_size" INT ,
  "referer" VARCHAR(128) ,
  "user_string" VARCHAR(256) ,
  "log_id" INT)
    LANGUAGE sql
    AS $$

select *
from log,access
where access.log_id =log.id and response_size <= sz

select function7(100)

$$

--- question 8 ---
create or replace function function8()
	   RETURNS TABLE( blockid varchar(32))
    LANGUAGE sql
    AS $$
with dailyAction as (select distinct  block_requested, type ,cast(log.time as date)
from log,blocks
where log.id = blocks.log_id and(
type = 'replicate' or type = 'Served'))

select distinct block_requested
from dailyAction
group by (block_requested, time)
having count(*) > 1
$$

select function8()

--- question 9 ---

create or replace function function9()
	   RETURNS TABLE( blockid varchar(32))
    LANGUAGE sql
    AS $$
with dailyAction as (select distinct  block_requested, type ,date_part('day', time) as day
,date_part('hour', time) as hour
from log,blocks
where log.id = blocks.log_id and(
type = 'replicate' or type = 'Served'))

select distinct block_requested
from dailyAction
group by (block_requested, day,hour)
having count(*) > 1
$$

select function9()



--- question 10 ---

create or replace function function10()
	   RETURNS TABLE(id int , user_string varchar)
    LANGUAGE sql
    AS $$
select log_id, user_string from
log, access
where log.id=access.log_id and user_string like '%Firefox%'

$$
select function10()

--- question 11 ---
create or replace function function11(meth varchar, time1 timestamp, time2 timestamp)
	   RETURNS TABLE(ip varchar)
    LANGUAGE sql
    AS $$ 
	select distinct source_ip
	from log,access
	where log.id=access.log_id and http_method=meth and time between time1 and time2
$$


select function11('POST', '01-01-1999', '31-12-2019')


--- question 12 ---
create or replace function function12(meth1 varchar, meth2 varchar, time1 timestamp, time2 timestamp)
	   RETURNS TABLE(ip varchar)
    LANGUAGE sql
    AS $$ 
	select distinct source_ip
	from log,access
	where log.id=access.log_id and http_method=meth1 and time between time1 and time2 
	and source_ip in (
      select distinct source_ip
      from log,access
      where log.id=access.log_id and http_method=meth2 and time between time1 and time2 
   )
$$

select function12('POST', 'GET', '01-01-1999', '31-12-2019')


--- question 13 ---

create or replace function function13(time1 timestamp, time2 timestamp)
	   RETURNS TABLE(ip varchar)
    LANGUAGE sql
    AS $$ 
	select source_ip
	from log,access
	where log.id=access.log_id and time between time1 and time2
	group by source_ip
	having count(distinct http_method)=4
$$

select function13( '01-01-1999', '31-12-2019')
