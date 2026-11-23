-- A posting's minimum salary should never exceed its maximum. This is the
-- employer's data, not a pipeline bug, so it warns instead of blocking the build.
{{ config(severity='warn') }}

select job_id, salary_min, salary_max
from {{ ref('dim_postings') }}
where salary_min > salary_max
