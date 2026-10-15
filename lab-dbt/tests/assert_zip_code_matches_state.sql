{{ config(severity='warn') }}

-- The zip code of a user must fall in one of the USPS ranges of its state.
-- The generated addresses are random, so this test only monitors the issue.
select u.user_id, u.state, u.zip_code
from {{ ref('stg_users') }} u
where not exists (
    select 1
    from {{ ref('zip_ranges') }} z
    where z.state = u.state
      and cast(u.zip_code as integer) between z.zip_min and z.zip_max
)
