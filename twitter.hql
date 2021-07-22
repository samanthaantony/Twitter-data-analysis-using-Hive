drop table Twitter;

create table Twitter (
  uID int,
  fID int
  )
row format delimited fields terminated by ',' stored as textfile;

load data local inpath '${hiveconf:G}' overwrite into table Twitter;

select RESULT, count(RESULT) as NumberOfUsers --For each group the subquery returns we count how many users belong to that group.This is the second map reduce.
FROM (select fID, count (fID) as RESULT --by grouping the followerId we count the number of userId the follower follows which is stored as RESULT.This is the first map reduce where ids in the reducer is the sequence of all users followed by the user with follower_id.
FROM Twitter GROUP BY fID) as TEMP --group the users by their number of the users they follow 
GROUP BY RESULT;
