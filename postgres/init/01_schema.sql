--- OLTP TABLE & TRIGGER

create table if not exists daily_bar(
    symbol varchar(20) not null, 
    trade_date date not null, 
    open numeric(18, 4),
    high numeric(18, 4),
    adj_close numeric(18, 4),
    low numeric(18, 4),
    close numeric(18, 4),
    volume bigint, 
    updated_at timestamptz not null default now(),
    primary key (symbol, trade_date)
);

create table if not exists instrument(
    symbol varchar(20) primary key,
    name text, 
    exchange varchar(20) not null default 'HOSE',
    sector text, 
    is_listed boolean not null default 'TRUE',
    updated_at timestamptz not null default now()
);

create table if not exists corporate_action(
    symbol varchar(20) not null, 
    action_date date not null, 
    action_type varchar(20) not null, 
    value numeric(18, 4),
    value_raw numeric(18, 4),
    updated_at timestamptz not null default now(),
    primary key(symbol, action_date, action_type)
);

create or replace function update_updated_at_column()
returns trigger as $$
begin 
    new.updated_at = now();
    return new;
end;
$$ language 'plpgsql';

create trigger update_daily_bar_updated_at before update on daily_bar
for each row
execute function update_updated_at_column();

create trigger update_instrument_updated_at before update on instrument
for each row
execute function update_updated_at_column();

create trigger update_corporate_action_updated_at before update on corporate_action
for each row
execute function update_updated_at_column();
