-- New postings per day and role. Salary uses only salaries the employer posted,
-- not Adzuna's estimates.

select
    cast(p.posted_at as date)              as posted_date,
    r.role,
    count(*)                               as postings,
    count(p.posted_salary_mid)             as postings_with_salary,
    avg(p.posted_salary_mid)               as avg_posted_salary_mid
from {{ ref('dim_postings') }} as p
inner join {{ ref('bridge_posting_roles') }} as r
    on p.job_id = r.job_id
where p.posted_at is not null
group by 1, 2
