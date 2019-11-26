
--- question 1 ---

create or replace function logtype (t1 timestamp , t2 timestamp )
   returns table (
      logtype VARCHAR,
      logcount BIGINT
)
as $$
begin
   return query select type, COUNT(*)
        from log
        where time between t1 and t2
        group by type
        order by count(*) desc;
end;
$$
language 'plpgsql';

create or replace procedure LogTypeAggregate(t1 timestamp , t2 timestamp )
language sql
as 
$$
select type, COUNT(*)
from log
where time between t1 and t2
group by type
order by count(*) desc
$$

call LogTypeAggregate('1999-11-01', '2019-03-05')

select type, COUNT(*)
from log
where time between '1999-11-01' and  '2019-03-05'
group by type
order by count(*) desc

--- question 2 ---

create or replace procedure LogPerDay(t1 timestamp , t2 timestamp )
language sql
as 
$$
select count(*),cast(time as date)
from log
where time between t1 and  t2
group by cast(time as date)
$$


select count(*),cast(time as date)
from log
where time between '2017-11-01' and  '2019-11-11'
group by cast(time as date)



--- question 3 ---

select COUNT(*),source_ip
from log
where  time between  '2016-01-29 00:00:00' and '2016-01-29 23:59:59'
group by source_ip



---question 4 ---

select block_requested
from blocks,log
where log.id = blocks.log_id 	