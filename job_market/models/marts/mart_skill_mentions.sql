-- How often each skill (seeds/skills.csv) is mentioned in postings, per role.
-- Adzuna only returns a ~500-character description snippet, so these shares are
-- a lower bound: a skill listed further down the posting is missed.

with postings as (
    select
        r.role,
        p.job_id,
        lower(p.title || ' ' || coalesce(p.description, '')) as body
    from {{ ref('dim_postings') }} as p
    inner join {{ ref('bridge_posting_roles') }} as r
        on p.job_id = r.job_id
),

role_totals as (
    select role, count(*) as role_postings
    from postings
    group by role
),

mentions as (
    select
        s.skill,
        s.skill_group,
        postings.role,
        count(distinct postings.job_id) as postings_mentioning
    from postings
    cross join {{ ref('skills') }} as s
    where {{ regex_contains('postings.body', 's.pattern') }}
    group by 1, 2, 3
)

select
    mentions.skill,
    mentions.skill_group,
    mentions.role,
    mentions.postings_mentioning,
    role_totals.role_postings,
    round(mentions.postings_mentioning / role_totals.role_postings, 3) as share_of_postings
from mentions
inner join role_totals
    on mentions.role = role_totals.role
