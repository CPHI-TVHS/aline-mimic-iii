drop temporary table if exists a;
drop temporary table if exists ve;
drop temporary table if exists serv;
drop temporary table if exists co;
drop table if exists ALINE_COHORT;

CREATE TEMPORARY TABLE a 
  select icustay_id, min(charttime) as starttime_aline
  from chartevents
  where icustay_id is not null
		and valuenum is not null
  and itemid in
  (
    51, 
    6701, 
    220050, 

    8368, 
    8555, 
    220051, 

    52, 
    6702, 
    220052, 
    225312 
  )
  group by icustay_id
  limit 1;

create temporary table ve as
  select icustay_id
  , sum( endtime-starttime)/24.0/60.0/60.0 as vent_day
    , min(starttime) as starttime_first
    , max(endtime) as endtime_last
  from ventilation_durations vd
  group by icustay_id
  limit 1;

create temporary table serv as
    select ie.icustay_id, se.curr_service
    , ROW_NUMBER() over (partition by ie.icustay_id order by se.transfertime DESC) as rn
    from icustays ie
    inner join services se
      on ie.hadm_id = se.hadm_id
      and se.transfertime < ie.intime + interval '2' hour
    limit 1;

create temporary table co
select
    ie.subject_id
    , ie.hadm_id
    , ie.icustay_id
    , ie.intime
    , to_char(ie.intime, 'day') as day_icu_intime
    , DAYOFWEEK(ie.intime) as day_icu_intime_num
    , extract(HOUR from ie.intime) as hour_icu_intime
    , ie.outtime
    , ROW_NUMBER() over (partition by ie.subject_id order by adm.admittime, ie.intime) as stay_num
    , UNIX_TIMESTAMP(ie.intime - pat.dob)/365.242/24.0/60.0/60.0 as age
    , pat.gender
    , case when pat.gender = 'M' then 1 else 0 end as gender_num
    , a.starttime_aline
    , case when a.starttime_aline is not null then 1 else 0 end as aline_flg
    , UNIX_TIMESTAMP(a.starttime_aline - ie.intime) / 24.0 / 60.0 /60.0 as aline_time_day
    , case
        when a.starttime_aline is not null
         and a.starttime_aline <= ie.intime + interval '1' hour
          then 1
        else 0
      end as initial_aline_flg

    -- ventilation
    , case when ve.icustay_id is not null then 1 else 0 end as vent_flg
    , case when ve.starttime_first < ie.intime + interval '12' hour then 1 else 0 end as vent_1st_12hr
    , case when ve.starttime_first < ie.intime + interval '24' hour then 1 else 0 end as vent_1st_24hr

    -- binary flag: were they ventilated before a-line insertion?
    , case
        when a.starttime_aline is not null and a.starttime_aline > ie.intime + interval '1' hour and ve.starttime_first<=a.starttime_aline then 1
        when a.starttime_aline is not null and a.starttime_aline > ie.intime + interval '1' hour and ve.starttime_first>a.starttime_aline then 0
        when a.starttime_aline is null 
        -- and ( (ve.starttime_first - ie.intime) <= interval '2' hour) 
        then 1
        when a.starttime_aline is null then 0 -- otherwise, ventilated 2 hours after admission
        else NULL
      end as vent_b4_aline

    -- number of days on a ventilator
    , ve.vent_day

    -- number of days free of ventilator after *last* extubation
    , UNIX_TIMESTAMP( (ie.outtime - ve.endtime_last))/24.0/60.0/60.0 as vent_free_day

    -- number of days *not* on a ventilator
    , UNIX_TIMESTAMP( (ie.outtime - ie.intime))/24.0/60.0/60.0 - vent_day as vent_off_day


    , ve.starttime_first as vent_starttime
    , ve.endtime_last as vent_endtime

    -- cohort flags // demographics
    , UNIX_TIMESTAMP  (ie.outtime - ie.intime)/24.0/60.0/60.0 as icu_los_day
    , UNIX_TIMESTAMP(adm.dischtime - adm.admittime)/24.0/60.0/60.0 as hospital_los_day
    , DAYOFWEEK(intime) as intime_dayofweek
    , extract(HOUR from intime) as intime_hour

    -- will be used to exclude patients in CSRU
    -- also only include those in CMED or SURG
    , s.curr_service as service_unit
    , case when s.curr_service like '%SURG' or s.curr_service like '%ORTHO%' then 1
          when s.curr_service = 'CMED' then 2
          when s.curr_service in ('CSURG','VSURG','TSURG') then 3
          else 0
        end
      as service_num

    -- outcome
    , case when adm.deathtime is not null then 1 else 0 end as hosp_exp_flg
    , case when adm.deathtime <= ie.outtime then 1 else 0 end as icu_exp_flg
    , case when pat.dod <= (ie.intime + interval '28' day) then 1 else 0 end as day_28_flg
    , UNIX_TIMESTAMP(pat.dod - adm.admittime)/24.0/60.0/60.0 as mort_day

    , case when pat.dod is null
        then 150 -- patient deaths are censored 150 days after admission
        else UNIX_TIMESTAMP(pat.dod - adm.admittime)/24.0/60.0/60.0
      end as mort_day_censored
    , case when pat.dod is null then 1 else 0 end as censor_flg

  from icustays ie
  inner join admissions adm
    on ie.hadm_id = adm.hadm_id
  inner join patients pat
    on ie.subject_id = pat.subject_id
  left join a
    on ie.icustay_id = a.icustay_id
  left join ve
    on ie.icustay_id = ve.icustay_id
  left join serv s
    on ie.icustay_id = s.icustay_id
    and s.rn = 1
  where ie.intime > (pat.dob + interval '16' year) -- only adults
	limit 1;

create table ALINE_COHORT
select
  co.*
from co
where stay_num = 1 -- first ICU stay
and icu_los_day > 1 -- one day in the ICU
and initial_aline_flg = 0 -- aline placed later than admission
and vent_starttime is not null -- were ventilated
and vent_starttime < intime + interval '12' hour -- ventilated within first 12 hours
and service_unit not in
(
  'CSURG','VSURG','TSURG' -- cardiac/vascular/thoracic surgery
  ,'NB'
  ,'NBB'
);
