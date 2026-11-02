--- OLTP TABLE & TRIGGER

create table if not exists daily_bar(
    symbol varchar(20) not null, 
    trade_date date not null, 
    open numeric(18, 4),
    high numeric(18, 4),
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
    ex_date date not null,                      -- ngày GDKHQ
    action_type varchar(20) not null,
    cash_amount numeric(18, 2),                 -- VND/cp, chỉ cash_dividend
    ratio numeric(10, 6),                       -- dạng thập phân: 100:15 -> 0.15
    subscription_price numeric(18, 2),          -- giá mua S (VND), chỉ rights_issue
    announce_date date,                         -- ngày công bố
    updated_at timestamptz not null default now(),
    primary key (symbol, ex_date, action_type),
    constraint chk_corporate_action_fields check (
        case action_type
            when 'cash_dividend'  then coalesce(cash_amount, 0) > 0 and ratio is null and subscription_price is null
            when 'stock_dividend' then cash_amount is null and coalesce(ratio, 0) > 0 and subscription_price is null
            when 'bonus_share'    then cash_amount is null and coalesce(ratio, 0) > 0 and subscription_price is null
            when 'rights_issue'   then cash_amount is null and coalesce(ratio, 0) > 0 and coalesce(subscription_price, 0) > 0
            else false
        end
    )
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
