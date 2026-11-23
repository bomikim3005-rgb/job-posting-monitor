-- One row per posting per search per ingest run, cleaned and renamed.
-- Adzuna wraps matched keywords in <strong> tags, so HTML is stripped here.

with source as (
    select * from {{ source('adzuna', 'postings') }}
)

select
    job_id,
    trim({{ strip_html('title') }})              as title,
    nullif(trim(company), '')                    as company,
    location,
    area,
    latitude,
    longitude,
    category,
    contract_type,
    contract_time,
    salary_min,
    salary_max,
    coalesce(salary_is_predicted, false)         as salary_is_predicted,
    created_at                                   as posted_at,
    redirect_url                                 as url,
    trim({{ strip_html('description') }})        as description,
    search_name                                  as role,
    ingested_at
from source
