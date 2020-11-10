


CREATE  VIEW ALINE_LABS as

-- count the number of blood gas measurements
-- abg_count - number of labs with pH/PCO2/PO2
-- vbg_count - number of times VBG appears in chartevents
with bg as
(
  select * from labevents limit 5
  -- abg_count
  -- vbg_count
)
-- we would like the *last* lab preceeding mechanical ventilation
, labs_preceeding as
(
  select co."SUBJECT_ID", co."HADM_ID"
    , l."VALUENUM", l."CHARTTIME"
    , case
            when "ITEMID" = 51006 then 'BUN'
            when "ITEMID" = 50806 then 'CHLORIDE'
            when "ITEMID" = 50902 then 'CHLORIDE'
            when "ITEMID" = 50912 then 'CREATININE'
            when "ITEMID" = 50811 then 'HEMOGLOBIN'
            when "ITEMID" = 51222 then 'HEMOGLOBIN'
            when "ITEMID" = 51265 then 'PLATELET'
            when "ITEMID" = 50822 then 'POTASSIUM'
            when "ITEMID" = 50971 then 'POTASSIUM'
            when "ITEMID" = 50824 then 'SODIUM'
            when "ITEMID" = 50983 then 'SODIUM'
            when "ITEMID" = 50803 then 'TOTALCO2' -- actually is 'BICARBONATE'
            when "ITEMID" = 50882 then 'TOTALCO2' -- actually is 'BICARBONATE'
            when "ITEMID" = 50804 then 'TOTALCO2'
            when "ITEMID" = 51300 then 'WBC'
            when "ITEMID" = 51301 then 'WBC'
          else null
        end as label
  from labevents l
  inner join ALINE_COHORT co
    on l."SUBJECT_ID" = co."SUBJECT_ID"
    and l."CHARTTIME" <= co.vent_starttime
    and l."CHARTTIME" >= co.vent_starttime - interval '1' day
  where l."ITEMID" in
  (
     51300,51301 -- wbc
    ,50811,51222 -- hgb
    ,51265 -- platelet
    ,50824, 50983 -- sodium
    ,50822, 50971 -- potassium
    ,50804 -- Total CO2 or ...
    ,50803, 50882  -- bicarbonate
    ,50806,50902 -- chloride
    ,51006 -- bun
    ,50912 -- creatinine
  )
)
, labs_rn as
(
  select
    "SUBJECT_ID", "HADM_ID", "VALUENUM", "label"
    , ROW_NUMBER() over (partition by "HADM_ID", label order by "CHARTTIME" DESC) as rn
  from labs_preceeding
)
, labs_grp as
(
  select
    "SUBJECT_ID", "HADM_ID"
    , max(case when label = 'BUN' then "VALUENUM" else null end) as BUN
    , max(case when label = 'CHLORIDE' then "VALUENUM" else null end) as CHLORIDE
    , max(case when label = 'CREATININE' then "VALUENUM" else null end) as CREATININE
    , max(case when label = 'HEMOGLOBIN' then "VALUENUM" else null end) as HEMOGLOBIN
    , max(case when label = 'PLATELET' then "VALUENUM" else null end) as PLATELET
    , max(case when label = 'POTASSIUM' then "VALUENUM" else null end) as POTASSIUM
    , max(case when label = 'SODIUM' then "VALUENUM" else null end) as SODIUM
    , max(case when label = 'TOTALCO2' then "VALUENUM" else null end) as TOTALCO2
    , max(case when label = 'WBC' then "VALUENUM" else null end) as WBC

  from labs_rn
  where rn = 1
  group by "SUBJECT_ID", "HADM_ID"
)
select co."SUBJECT_ID", co."HADM_ID"
  , lg.bun as bun_first
  , lg.chloride as chloride_first
  , lg.creatinine as creatinine_first
  , lg.HEMOGLOBIN as hgb_first
  , lg.platelet as platelet_first
  , lg.potassium as potassium_first
  , lg.sodium as sodium_first
  , lg.TOTALCO2 as tco2_first
  , lg.wbc as wbc_first

from ALINE_COHORT co
left join labs_grp lg
  on co."HADM_ID" = lg."HADM_ID"
