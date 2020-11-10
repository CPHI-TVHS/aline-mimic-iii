-- This query defines the cohort used for the ALINE study.

-- Inclusion criteria:
--  adult patients
--  In ICU for at least 24 hours
--  First ICU admission
--  mechanical ventilation within the first 12 hours
--  medical or surgical ICU admission

-- Exclusion criteria:
--  **Angus sepsis
--  **On vasopressors (?is this different than on dobutamine)
--  IAC placed before admission
--  CSRU patients

-- **These exclusion criteria are applied in the data.sql file.

-- This query also extracts demographics, and necessary preliminary flags needed
-- for data extraction. For example, since all data is extracted before
-- ventilation, we need to extract start times of ventilation


-- This query requires the following tables:
--  ventdurations - extracted by mimic-code/etc/ventilation-durations.sql


DROP MATERIALIZED VIEW IF EXISTS ALINE_COHORT CASCADE;
CREATE MATERIALIZED VIEW ALINE_COHORT as

-- get start time of arterial line
-- Definition of arterial line insertion:
--  First measurement of invasive blood pressure
with a as
(
  select "ICUSTAY_ID"
  , min("CHARTTIME") as starttime_aline
  from chartevents
  where "ICUSTAY_ID" is not null
  and "VALUENUM" is not null
  and "ITEMID" in
  (
    51, --	Arterial BP [Systolic]
    6701, --	Arterial BP #2 [Systolic]
    220050, --	Arterial Blood Pressure systolic

    8368, --	Arterial BP [Diastolic]
    8555, --	Arterial BP #2 [Diastolic]
    220051, --	Arterial Blood Pressure diastolic

    52, --"Arterial BP Mean"
    6702, --	Arterial BP Mean #2
    220052, --"Arterial Blood Pressure mean"
    225312 --"ART BP mean"
  )
  group by "ICUSTAY_ID"
)
-- first time ventilation was started
-- last time ventilation was stopped
, ve as
(
  select "icustay_id"
    , sum(extract(epoch from endtime-starttime))/24.0/60.0/60.0 as vent_day
    , min(starttime) as starttime_first
    , max(endtime) as endtime_last
  from public."VENTDURATIONS" vd
  group by "icustay_id"
)
, serv as
(
    select ie."ICUSTAY_ID", se."CURR_SERVICE"
    , ROW_NUMBER() over (partition by ie."ICUSTAY_ID" order by se."TRANSFERTIME" DESC) as rn
    from icustays ie
    inner join services se
      on ie."HADM_ID" = se."HADM_ID"
      and se."TRANSFERTIME" < ie."INTIME" + interval '2' hour
)
-- cohort view - used to define other concepts
, co as
(
  select
    ie."SUBJECT_ID", ie."HADM_ID", ie."ICUSTAY_ID"
    , ie."INTIME"
    , to_char(ie."INTIME", 'day') as day_icu_intime
    , extract(dow from ie."INTIME") as day_icu_intime_num
    , extract(hour from ie."INTIME") as hour_icu_intime
    , ie."OUTTIME"

    , ROW_NUMBER() over (partition by ie."SUBJECT_ID" order by adm."ADMITTIME", ie."INTIME") as stay_num
    , extract(epoch from (ie."INTIME" - pat."DOB"))/365.242/24.0/60.0/60.0 as age
    , pat."GENDER"
    , case when pat."GENDER" = 'M' then 1 else 0 end as gender_num

    -- TODO: weight_first, height_first, bmi
    --  bmi

    -- service

    -- collapse ethnicity into fixed categories

    -- time of a-line
    , a.starttime_aline
    , case when a.starttime_aline is not null then 1 else 0 end as aline_flg
    , extract(epoch from (a.starttime_aline - ie."INTIME"))/24.0/60.0/60.0 as aline_time_day
    , case
        when a.starttime_aline is not null
         and a.starttime_aline <= ie."INTIME" + interval '1' hour
          then 1
        else 0
      end as initial_aline_flg

    -- ventilation
    , case when ve."icustay_id" is not null then 1 else 0 end as vent_flg
    , case when ve.starttime_first < ie."INTIME" + interval '12' hour then 1 else 0 end as vent_1st_12hr
    , case when ve.starttime_first < ie."INTIME" + interval '24' hour then 1 else 0 end as vent_1st_24hr

    -- binary flag: were they ventilated before a-line insertion?
    , case
        when a.starttime_aline is not null and a.starttime_aline > ie."INTIME" + interval '1' hour and ve.starttime_first<=a.starttime_aline then 1
        when a.starttime_aline is not null and a.starttime_aline > ie."INTIME" + interval '1' hour and ve.starttime_first>a.starttime_aline then 0
        when a.starttime_aline is null and ( (ve.starttime_first-ie."INTIME") <= interval '2' hour) then 1
        when a.starttime_aline is null then 0 -- otherwise, ventilated 2 hours after admission
        else NULL
      end as vent_b4_aline

    -- number of days on a ventilator
    , ve.vent_day

    -- number of days free of ventilator after *last* extubation
    , extract(epoch from (ie."OUTTIME" - ve.endtime_last))/24.0/60.0/60.0 as vent_free_day

    -- number of days *not* on a ventilator
    , extract(epoch from (ie."OUTTIME" - ie."INTIME"))/24.0/60.0/60.0 - vent_day as vent_off_day


    , ve.starttime_first as vent_starttime
    , ve.endtime_last as vent_endtime

    -- cohort flags // demographics
    , extract(epoch from (ie."OUTTIME"  - ie."INTIME"))/24.0/60.0/60.0 as icu_los_day
    , extract(epoch from (adm."DISCHTIME" - adm."ADMITTIME"))/24.0/60.0/60.0 as hospital_los_day
    , extract('dow' from "INTIME") as intime_dayofweek
    , extract('hour' from "INTIME") as intime_hour

    -- will be used to exclude patients in CSRU
    -- also only include those in CMED or SURG
    , s."CURR_SERVICE" as service_unit
    , case when s."CURR_SERVICE" like '%SURG' or s."CURR_SERVICE" like '%ORTHO%' then 1
          when s."CURR_SERVICE" = 'CMED' then 2
          when s."CURR_SERVICE" in ('CSURG','VSURG','TSURG') then 3
          else 0
        end
      as service_num

    -- outcome
    , case when adm."DEATHTIME" is not null then 1 else 0 end as hosp_exp_flg
    , case when adm."DEATHTIME" <= ie."OUTTIME" then 1 else 0 end as icu_exp_flg
    , case when pat."DOD" <= (ie."INTIME" + interval '28' day) then 1 else 0 end as day_28_flg
    , extract(epoch from (pat."DOD" - adm."ADMITTIME"))/24.0/60.0/60.0 as mort_day

    , case when pat."DOD" is null
        then 150 -- patient deaths are censored 150 days after admission
        else extract(epoch from (pat."DOD" - adm."ADMITTIME"))/24.0/60.0/60.0
      end as mort_day_censored
    , case when pat."DOD" is null then 1 else 0 end as censor_flg

  from icustays ie
  inner join admissions adm
    on ie."HADM_ID" = adm."HADM_ID"
  inner join patients pat
    on ie."SUBJECT_ID" = pat."SUBJECT_ID"
  left join a
    on ie."ICUSTAY_ID" = a."ICUSTAY_ID"
  left join ve
    on ie."ICUSTAY_ID" = ve."icustay_id"
  left join serv s
    on ie."ICUSTAY_ID" = s."ICUSTAY_ID"
    and s.rn = 1
  where ie."INTIME" > (pat."DOB" + interval '16' year) -- only adults
)
select
  co.*
from co
where stay_num = 1 -- first ICU stay
and icu_los_day > 1 -- one day in the ICU
and initial_aline_flg = 0 -- aline placed later than admission
and vent_starttime is not null -- were ventilated
and vent_starttime < "INTIME" + interval '12' hour -- ventilated within first 12 hours
and service_unit not in
(
  'CSURG','VSURG','TSURG' -- cardiac/vascular/thoracic surgery
  ,'NB'
  ,'NBB'
);
--  TODO: can't define medical or surgical ICU admission using ICU service type


-- Recall, two exclusion criteria are applied in data.sql:
--  **Angus sepsis
--  **On vasopressors (?is this different than on dobutamine)
