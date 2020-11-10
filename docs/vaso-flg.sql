-- Create a table which indicates if a patient was ever on a vasopressor during their ICU stay

-- List of vasopressors used:
-- norepinephrine - 30047,30120,221906
-- epinephrine - 30044,30119,30309,221289
-- phenylephrine - 30127,30128,221749
-- vasopressin - 30051,222315
-- dopamine - 30043,30307,221662
-- Isuprel - 30046,227692


CREATE  VIEW ALINE_VASO_FLG as
with io_cv as
(
  select
    "ICUSTAY_ID", "CHARTTIME", "ITEMID", "STOPPED", "RATE", "AMOUNT"
  from public."inputevents_cv"
  where "ITEMID" in
  (
    30047,30120 -- norepinephrine
    ,30044,30119,30309 -- epinephrine
    ,30127,30128 -- phenylephrine
    ,30051 -- vasopressin
    ,30043,30307,30125 -- dopamine
    ,30046 -- isuprel
  )
  and "RATE" is not null
  and "RATE" > 0
)
-- select only the "ITEMID"s from the inputevents_mv table related to vasopressors
, io_mv as
(
  select
    "ICUSTAY_ID", "LINKORDERID", "STARTTIME", "ENDTIME"
  from public."inputevents_mv" io
  -- Subselect the vasopressor "ITEMID"s
  where "ITEMID" in
  (
  221906 -- norepinephrine
  ,221289 -- epinephrine
  ,221749 -- phenylephrine
  ,222315 -- vasopressin
  ,221662 -- dopamine
  ,227692 -- isuprel
  )
  and "RATE" is not null
  and "RATE" > 0
  and "STATUSDESCRIPTION" != 'Rewritten' -- only valid orders
)
select
  co."SUBJECT_ID", co."HADM_ID", co."ICUSTAY_ID"
  , max(case when coalesce(io_mv."ICUSTAY_ID", io_cv."ICUSTAY_ID") is not null then 1 else 0 end) as vaso_flg
from aline_cohort co
left join io_mv
  on co."ICUSTAY_ID" = io_mv."ICUSTAY_ID"
left join io_cv
  on co."ICUSTAY_ID" = io_cv."ICUSTAY_ID"
group by co."SUBJECT_ID", co."HADM_ID", co."ICUSTAY_ID"
order by "ICUSTAY_ID";
