--- REPLICA IDENTITY FULL 
alter table daily_bar replica identity full;
alter table instrument replica identity full;
alter table corporate_action replica identity full;

create publication dbz_publication for table daily_bar, instrument, corporate_action;
