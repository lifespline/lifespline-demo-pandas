-- vendor: AWS Redshift
-- create table
create table if not exists empower.test_glue.training (
	id int,
	a varchar(50),
	b varchar(50)
);
truncate table empower.test_glue.training;
insert into empower.test_glue.training
values (1, 'a1', 'b1'), (2, 'a2', 'b1'), (3, 'a1', 'b2'), (4, 'a2', 'b2'), (4, 'a2', 'b2');

select * from empower.test_glue.training

-----

select a, b, count(id)
from empower.test_glue.training
group by a, b
--a1	b1	1
--a2	b1	1
--a1	b2	1
--a2	b2	2

-----

select a, b, count(distinct id)
from empower.test_glue.training
group by a, b
--a1	b1	1
--a2	b1	1
--a1	b2	1
--a2	b2	1

-----

select a, b, '[' || listagg(id, ',') || ']'
from empower.test_glue.training
group by a, b
--a1	b2	3
--a1	b1	1
--a2	b1	2
--a2	b2	4,4

-----
-- unfinished

CREATE temporary table seq_0_to_3 AS (
  SELECT 0 AS i UNION ALL                                      
  SELECT 1 UNION ALL
  SELECT 2 UNION ALL    
  SELECT 3          
);

WITH exploded_array AS (   
  -- explodes the stringified array
  SELECT
    json_extract_array_element_text('[e1,e2,e3,e4]', seq.i) AS json_output,
    seq.i
  FROM
    seq_0_to_3 AS seq
  WHERE
    seq.i < JSON_ARRAY_LENGTH('[e1,e2,e3,e4]')
)
SELECT *
FROM exploded_array;

-- exec
with recursive
	numbers(NUMBER) as (
		select 1
		UNION ALL
		select NUMBER + 1
		
		from numbers
		where NUMBER < (select count(*) from seq_0_to_3)
	)
select *
from numbers

-----
select
	id,
	a,
	b, 
	last_value(b) over (
		partition by
			id, a
		order by
			b asc
		rows between
			unbounded preceding
		and unbounded following
	) as lst
from empower.test_glue.training