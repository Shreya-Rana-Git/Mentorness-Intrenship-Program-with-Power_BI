create database game_analysis;
use game_analysis;

-- Problem Statement - Game Analysis dataset
-- 1) Players play a game divided into 3-levels (L0,L1 and L2)
-- 2) Each level has 3 difficulty levels (Low,Medium,High)
-- 3) At each level,players have to kill the opponents using guns/physical fight
-- 4) Each level has multiple stages at each difficulty level.
-- 5) A player can only play L1 using its system generated L1_code.
-- 6) Only players who have played Level1 can possibly play Level2 
--    using its system generated L2_code.
-- 7) By default a player can play L0.
-- 8) Each player can login to the game using a Dev_ID.
-- 9) Players can earn extra lives at each stage in a level.

select * from pd;

select * from ld;


alter table pd modify L1_Status varchar(30);
alter table pd modify L2_Status varchar(30);
alter table pd modify P_ID int primary key;
alter table pd drop myunknowncolumn;

alter table ld drop myunknowncolumn;
alter table ld change timestamp start_datetime datetime;
alter table ld modify Dev_Id varchar(10);
alter table ld modify Difficulty varchar(15);
alter table ld add primary key(P_ID,Dev_id,start_datetime);

-- pd (P_ID,PName,L1_status,L2_Status,L1_code,L2_Code)
-- ld (P_ID,Dev_ID,start_time,stages_crossed,level,difficulty,kill_count,
-- headshots_count,score,lives_earned)



-- Q1) Extract P_ID,Dev_ID,PName and Difficulty_level of all players 
-- at level 0

select ld.P_ID,ld.Dev_ID,pd.Pname,ld.Difficulty from ld inner join pd on ld.P_ID = pd.P_ID where Level=0;


-- Q2) Find Level1_code wise Avg_Kill_Count where lives_earned is 2 and atleast
--    3 stages are crossed

select  pd.L1_Code as Level1_code , avg(ld.kill_count) as Avg_Kill_Count 
from pd inner join ld on pd.p_id = ld.p_id 
where lives_earned = 2 and stages_crossed >=3 
group by pd.L1_Code;



-- Q3) Find the total number of stages crossed at each diffuculty level
-- where for Level2 with players use zm_series devices. Arrange the result
-- in decsreasing order of total number of stages crossed.

select ld.Difficulty , count(ld.Stages_crossed) 
from ld inner join pd
on ld.p_id = pd.p_id
where ld.level = 2 and ld.dev_id like 'zm%' 
group by(ld.Difficulty) order by count(ld.Stages_crossed) desc ;




-- Q4) Extract P_ID and the total number of unique dates for those players 
-- who have played games on multiple days.

select distinct P_ID,COUNT( date(start_datetime)) as Total_Unique_Dates
from ld
group by P_ID
having COUNT(distinct date(start_datetime)) > 1;



-- Q5) Find P_ID and level wise sum of kill_counts where kill_count
-- is greater than avg kill count for the Medium difficulty.



select ld.p_id,ld.level,sum(ld.kill_count) as total_kill_count
from ld inner join (select level, avg(kill_count) as avg_kill_count from ld  where level='Medium' group by level) 
as avg_diff_level
on ld.level = avg_diff_level.level
where kill_count>avg_diff_level.avg_kill_count
group by ld.p_id,ld.level;





-- Q6)  Find Level and its corresponding Level code wise sum of lives earned 
-- excluding level 0. Arrange in asecending order of level.

select level,sum(lives_earned)
from ld
where level <> 0 
group by level
order by level asc;




-- Q7) Find Top 3 score based on each dev_id and Rank them in increasing order
-- using Row_Number. Display difficulty as well. 

select dev_id, difficulty, score, rank_ 
from (
select dev_id, difficulty, score, row_number() over(partition by dev_id order by score desc) as rank_ 
from ld
)
as ranked_scores 
where rank_<=3;




-- Q8) Find first_login datetime for each device id

select dev_id, min(start_datetime) as 'first_login datetime'
from ld
group by dev_id;

-- Q9) Find Top 5 score based on each difficulty level and Rank them in 
-- increasing order using Rank. Display dev_id as well.

select difficulty,dev_id, score, rank_
from(
select difficulty,dev_id, score, rank() over(partition by  difficulty order by score desc ) rank_ 
from ld
)
as ranked_score
where rank_<=5;




-- Q10) Find the device ID that is first logged in(based on start_datetime) 
-- for each player(p_id). Output should contain player id, device id and 
-- first login datetime.

select p_id as 'player id' , dev_id as 'device id', min(start_datetime) as 'first login datetime'
from ld
group by p_id , dev_id;



-- Q11) For each player and date, how many kill_count played so far by the player. That is, the total number of games played 
-- by the player until that date.
-- a) window function

select distinct p_id,date(start_datetime) as 'date', sum(kill_count) 
over(partition by p_id order by date(start_datetime)) as 'total_kill_count' from ld ;

-- b) without window function

select
    p_id,
    date(start_datetime) as 'date',
    sum(kill_count) as Total_Kill_Count
from ld as l1
where start_datetime <= (
    select max(start_datetime)
    from ld as l2
    where l1.p_id = l2.p_id
    and date(l1.start_datetime) = date(l2.start_datetime)
)
group by p_id, date(start_datetime);



-- Q12) Find the cumulative sum of stages crossed over a start_datetime 

select p_id,start_datetime,sum(Stages_crossed) 
over(partition by p_id order by start_datetime)as 'cumulative sum of stages crossed' from ld;



-- Q13) Find the cumulative sum of an stages crossed over a start_datetime
-- for each player id but exclude the most recent start_datetime

select p_id, start_datetime, sum(Stages_crossed) 
over(partition by p_id order by start_datetime) as 'sum of an stages crossed' from  ld 
where start_datetime not in (select date_time from ( select p_id,max(start_datetime) as date_time from ld 
group by p_id)as date_table);



-- Q14) Extract top 3 highest sum of score for each device id and the corresponding player_id

select distinct p_id, dev_id, total_score  , rank_ from (
select p_id, dev_id, sum(score) as total_score  ,
dense_rank() over(partition by dev_id order by sum(score) desc) as rank_ from ld group by p_id,dev_id
) as rankes_score where rank_ <=3;


-- Q15) Find players who scored more than 50% of the avg score scored by sum of 
-- scores for each player_id

select p_id,pname
from (
    select 
        ld.p_id, pd.pname,
        sum(score) as Total_Score,
        avg(sum(score)) over (partition by p_id) as Avg_Total_Score
    from ld join pd on ld.p_id=pd.p_id
    group by P_ID
) as ScoreSummary
where Total_Score > 0.5 * Avg_Total_Score;


-- Q16) Create a stored procedure to find top n headshots_count based on each dev_id and Rank them in increasing order
-- using Row_Number. Display difficulty as well.

DELIMITER //

create procedure find_headshots_count(in n int)
begin
	select dev_id, difficulty, count_ ,rank_ from( 
    select dev_id, difficulty, Headshots_Count as count_ ,
    row_number() over(partition by dev_id order by Headshots_Count asc)as rank_ from ld ) 
    as ranked_headshot_count where rank_<=n;
end//



call find_headshots_count(3)//

-- drop procedure if exists find_headshots_count//

DELIMITER ;


-- Q17) Create a function to return sum of Score for a given player_id.


desc ld;

DELIMITER //

create procedure sum_of_score_of_the_player(in pID int)
begin
	select p_id, sum(score) as total_score  from ld where p_id=pId group by p_id;  
end//

call sum_of_score_of_the_player(211)//
DELIMITER ;

-- drop procedure if exists sum_of_score_of_the_player;