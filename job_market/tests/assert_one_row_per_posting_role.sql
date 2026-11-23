-- bridge_posting_roles must have one row per (job_id, role).
select job_id, role, count(*) as n
from {{ ref('bridge_posting_roles') }}
group by job_id, role
having count(*) > 1
