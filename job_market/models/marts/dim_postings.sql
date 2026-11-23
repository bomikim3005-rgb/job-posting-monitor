-- One row per posting: its latest attributes plus when the pipeline first and
-- last saw it. A posting found by several searches still appears once here;
-- bridge_posting_roles holds the posting-to-role mapping.

with snapshots as (
    select * from {{ ref('stg_adzuna__postings') }}
),

seen as (
    select
        job_id,
        min(ingested_at)             as first_seen_at,
        max(ingested_at)             as last_seen_at,
        count(distinct ingested_at)  as times_seen
    from snapshots
    group by job_id
),

ranked as (
    select
        *,
        row_number() over (partition by job_id order by ingested_at desc, role) as rn
    from snapshots
)

select
    ranked.job_id,
    ranked.title,
    ranked.company,
    ranked.location,
    ranked.area,
    ranked.latitude,
    ranked.longitude,
    ranked.category,
    ranked.contract_type,
    ranked.contract_time,
    ranked.salary_min,
    ranked.salary_max,
    ranked.salary_is_predicted,
    case
        when not ranked.salary_is_predicted
            then (ranked.salary_min + ranked.salary_max) / 2
    end                                  as posted_salary_mid,
    ranked.posted_at,
    ranked.url,
    ranked.description,
    seen.first_seen_at,
    seen.last_seen_at,
    seen.times_seen
from ranked
inner join seen on ranked.job_id = seen.job_id
where ranked.rn = 1
