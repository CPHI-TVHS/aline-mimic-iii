
CREATE  VIEW ALINE_VITALS as

-- first, group together "ITEMID"s for the same vital sign
with vitals_stg0 as
(
  select
    co."SUBJECT_ID", co."HADM_ID", "CHARTTIME"
    , case
        -- MAP, Temperature, HR, CVP, SpO2,
        when "ITEMID" in (456,52,6702,443,220052,220181,225312) then 'MAP'
        when "ITEMID" in (223762,676,223761,678) then 'Temperature'
        when "ITEMID" in (211,220045) then 'HeartRate'
        when "ITEMID" in (113,220074) then 'CVP'
        when "ITEMID" in (646,220277) then 'SpO2'
      else null end as label
    -- convert F to C
    , case when "ITEMID" in (223761,678) then ("VALUENUM"-32)/1.8 else "VALUENUM" end as valuenum
  from ALINE_COHORT co
  inner join chartevents ce
    on ce."SUBJECT_ID" = co."SUBJECT_ID"
    and ce."CHARTTIME" <= co.vent_starttime
    and ce."CHARTTIME" >= co.vent_starttime - interval '1' day
    and "ITEMID" in
    (
        456,52,6702,443,220052,220181,225312 -- map
      , 223762,676,223761,678 -- temp
      , 211,220045 -- hr
      , 113,220074 -- cvp
      , 646,220277 -- spo2
    )
)
-- next, assign an integer where rn=1 is the vital sign just preceeding vent
, vitals_stg1 as
(
  select
    "SUBJECT_ID", "HADM_ID", label
    , case when label = 'MAP' then valuenum else null end as MAP
    , case when label = 'Temperature' then valuenum else null end as Temperature
    , case when label = 'HeartRate' then valuenum else null end as HeartRate
    , case when label = 'CVP' then valuenum else null end as CVP
    , case when label = 'SpO2' then valuenum else null end as SpO2
    , ROW_NUMBER() over (partition by "HADM_ID", label order by "CHARTTIME" DESC) as rn
  from vitals_stg0
)
-- now aggregate where rn=1 to give the vital sign just before the vent starttime
, vitals as
(
  select
    "SUBJECT_ID", "HADM_ID", rn
    , min(MAP) as MAP
    , min(Temperature) as Temperature
    , min(HeartRate) as HeartRate
    , min(CVP) as CVP
    , min(SpO2) as SpO2
  from vitals_stg1
  group by "SUBJECT_ID", "HADM_ID", rn
  having rn = 1
)
select
  co."SUBJECT_ID", co."HADM_ID"
  , v.MAP, v.Temperature, v.HeartRate, v.CVP, v.SpO2
from ALINE_COHORT co
left join vitals v
  on co."HADM_ID" = v."HADM_ID"
