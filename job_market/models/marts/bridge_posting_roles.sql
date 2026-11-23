-- Which roles (searches) each posting matched. A "Data Scientist / Analytics
-- Engineer" posting can belong to two roles.

select distinct
    job_id,
    role
from {{ ref('stg_adzuna__postings') }}
