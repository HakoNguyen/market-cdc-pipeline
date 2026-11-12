{#
  Ghi đè macro cùng tên của dbt-starrocks 1.12.2. Bản gốc sinh `UPDATE t, (subquery) SET ...`
  (cú pháp MySQL) mà StarRocks không hiểu. Bảng snapshot lại là DUPLICATE KEY, nên UPDATE kiểu
  nào cũng không chạy. Thay bằng INSERT OVERWRITE: đọc lại cả bảng, điền dbt_valid_to cho các
  bản vừa bị thay thế, rồi ghi đè. Bước insert bản mới vẫn dùng macro gốc của adapter.
  shortcut: ghi lại toàn bộ bảng mỗi lần snapshot, ổn với vài trăm dòng;
  khi bảng lớn thì đổi bảng snapshot sang PRIMARY KEY và dùng UPDATE ... FROM.
#}
{% macro starrocks__snapshot_merge_sql_update(target, source, insert_cols) -%}
    insert overwrite {{ target }} ({{ insert_cols | join(', ') }})
    select
    {%- for column in insert_cols %}
        {%- if column | replace('`', '') | lower == 'dbt_valid_to' %}
        case when s.dbt_scd_id is not null and t.dbt_valid_to is null
             then s.dbt_valid_to else t.dbt_valid_to end
        {%- else %}
        t.{{ column }}
        {%- endif %}{{ "," if not loop.last }}
    {%- endfor %}
    from {{ target }} t
    left join (
        select dbt_scd_id, dbt_valid_to
        from {{ source }}
        where dbt_change_type = 'update'
    ) s on s.dbt_scd_id = t.dbt_scd_id
{%- endmacro %}
