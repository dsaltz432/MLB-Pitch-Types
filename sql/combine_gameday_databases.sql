attach '/Users/danielsaltz/Documents/MLB_Project/data/mlb-gameday/mlb-gameday3.db' as toMerge;
BEGIN;
insert into action select * from toMerge.action;
insert into atbat select * from toMerge.atbat;
insert into pitch select * from toMerge.pitch;
insert into po select * from toMerge.po;
insert into runner select * from toMerge.runner;
COMMIT;
detach toMerge;

