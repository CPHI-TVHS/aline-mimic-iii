-- This query extracts the sequential organ failure assessment (formally: sepsis-related organ failure assessment).
-- This query is *specifically designed for the arterial line study*.
-- It makes many assumptions which are only valid in that cohort: no patients are on vasopressors, no patients are ventilated during data extraction.

-- Reference for SOFA:
--    Jean-Louis Vincent, Rui Moreno, Jukka Takala, Sheila Willatts, Arnaldo De Mendonça,
--    Hajo Bruining, C. K. Reinhart, Peter M Suter, and L. G. Thijs.
--    "The SOFA (Sepsis-related Organ Failure Assessment) score to describe organ dysfunction/failure."
--    Intensive care medicine 22, no. 7 (1996): 707-710.

-- Variables used in SOFA:
--  GCS, MAP, FiO2, Ventilation status (sourced from CHARTEVENTS)
--  Creatinine, Bilirubin, FiO2, PaO2, Platelets (sourced from LABEVENTS)
--  Dobutamine, Epinephrine, Norepinephrine (sourced from INPUTEVENTS_MV and INPUTEVENTS_CV)
--  Urine output (sourced from OUTPUTEVENTS)


CREATE  VIEW ALINE_SOFA AS
-- extract PaO2/FiO2
-- do not need to worry about patient ventilation
with stg_fio2 as
(
  select co."ICUSTAY_ID", ce."CHARTTIME"
    -- pre-process the FiO2s to ensure they are between 21-100%
    , max(
        case
          when "ITEMID" = 223835
            then case
              when "VALUENUM" > 0 and "VALUENUM" <= 1
                then "VALUENUM" * 100
              -- improperly input data - looks like O2 flow in litres
              when "VALUENUM" > 1 and "VALUENUM" < 21
                then null
              when "VALUENUM" >= 21 and "VALUENUM" <= 100
                then "VALUENUM"
              else null end -- unphysiological
        when "ITEMID" in (3420, 3422)
        -- all these values are well formatted
            then "VALUENUM"
        when "ITEMID" = 190 and "VALUENUM" > 0.20 and "VALUENUM" < 1
        -- well formatted but not in %
            then "VALUENUM" * 100
      else null end
    ) as fio2_chartevents
  from aline_cohort co
  left join CHARTEVENTS ce
    on co."ICUSTAY_ID" = ce."ICUSTAY_ID"
    and ce."ITEMID" in
    (
      3420 -- FiO2
    , 190 -- FiO2 set
    , 223835 -- Inspired O2 Fraction (FiO2)
    , 3422 -- FiO2 [measured]
    )
    group by co."ICUSTAY_ID", ce."CHARTTIME"
)
, bg as
(
    select pvt."ICUSTAY_ID", pvt."CHARTTIME"
  , max(case when label = 'SPECIMEN' then "VALUE" else null end) as SPECIMEN
  , max(case when label = 'FIO2' then "VALUENUM" else null end) as FIO2
  , max(case when label = 'PO2' then "VALUENUM" else null end) as PO2
  from
  ( -- begin query that extracts the data
    select co."ICUSTAY_ID", "CHARTTIME"
    -- here we assign labels to ITEMIDs
    -- this also fuses together multiple ITEMIDs containing the same data
        , case
          when "ITEMID" = 50800 then 'SPECIMEN'
          when "ITEMID" = 50816 then 'FIO2'
          when "ITEMID" = 50821 then 'PO2'
          else null
          end as label
          , "VALUE"
          -- add in some sanity checks on the values
          , case
              when "VALUENUM" <= 0 then null
              when "ITEMID" = 50816 and "VALUENUM" > 100 then null -- FiO2
              -- conservative upper limit
              when "ITEMID" = 50821 and "VALUENUM" > 800 then null -- PO2
          else "VALUENUM"
          end as "VALUENUM"

      from aline_cohort co
      left join labevents le
        on co."SUBJECT_ID" = le."SUBJECT_ID"
        and le."CHARTTIME" between co.vent_starttime - interval '1' day and co.vent_starttime
        and le."ITEMID" in (50800, 50816, 50821)
  ) pvt
  group by pvt."ICUSTAY_ID", pvt."CHARTTIME"
  -- we only want rows with a PO2 measurement
  having max(case when label = 'PO2' then "VALUENUM" else null end) is not null
)
, stg_pafi as
(
select
  bg."ICUSTAY_ID", bg."CHARTTIME"
  , bg.PO2, bg.FIO2, s2.fio2_chartevents
  , case when coalesce(bg.FIO2, s2.fio2_chartevents, 0) > 0 and coalesce(bg.FIO2, s2.fio2_chartevents, 100) < 100
        then 100*bg.PO2/(coalesce(bg.FIO2, s2.fio2_chartevents))
      else null end as pao2fio2
  , ROW_NUMBER() over (partition by bg."ICUSTAY_ID" order by bg."CHARTTIME" DESC, s2."CHARTTIME" DESC) as rn
from bg
left join stg_fio2 s2
  -- same patient
  on  bg."ICUSTAY_ID" = s2."ICUSTAY_ID"
  -- fio2 occurred at most 4 hours before this blood gas
  and s2."CHARTTIME" between bg."CHARTTIME" - interval '4' hour and bg."CHARTTIME"
where coalesce(bg.SPECIMEN,'ART') = 'ART'
)

-------------------------------------------
-- LABS --
-------------------------------------------
, labs as (
select
  pvt."ICUSTAY_ID"
  , max(case when label = 'BILIRUBIN' then "VALUENUM" else null end) as BILIRUBIN_max
  , max(case when label = 'CREATININE' then "VALUENUM" else null end) as CREATININE_max
  , min(case when label = 'PLATELET' then "VALUENUM" else null end) as PLATELET_min
from
( -- begin query that extracts the data
  select co."ICUSTAY_ID"
  -- here we assign labels to ITEMIDs
  -- this also fuses together multiple ITEMIDs containing the same data
  , case
        when "ITEMID" = 50885 then 'BILIRUBIN'
        when "ITEMID" = 50912 then 'CREATININE'
        when "ITEMID" = 51265 then 'PLATELET'
      else null
    end as label
  , -- add in some sanity checks on the values
  -- the where clause below requires all "VALUENUM" to be > 0, so these are only upper limit checks
    case
      when "ITEMID" = 50885 and "VALUENUM" >   150 then null -- mg/dL 'BILIRUBIN'
      when "ITEMID" = 50912 and "VALUENUM" >   150 then null -- mg/dL 'CREATININE'
      when "ITEMID" = 51265 and "VALUENUM" > 10000 then null -- K/uL 'PLATELET'
    else le."VALUENUM"
    end as "VALUENUM"

  from aline_cohort co

  left join labevents le
    on co."SUBJECT_ID" = le."SUBJECT_ID"
    and le."CHARTTIME" between co.vent_starttime - interval '1' day and co.vent_starttime
    and le."ITEMID" in
    (
      -- comment is: LABEL | CATEGORY | FLUID | NUMBER OF ROWS IN LABEVENTS
      50885, -- BILIRUBIN, TOTAL | CHEMISTRY | BLOOD | 238277
      50912, -- CREATININE | CHEMISTRY | BLOOD | 797476
      51265  -- PLATELET COUNT | HEMATOLOGY | BLOOD | 778444
    )
    and "VALUENUM" is not null and "VALUENUM" > 0 -- lab values cannot be 0 and cannot be negative
) pvt
group by pvt."ICUSTAY_ID"
)

-- VITALS --
, vitals as
(
  select
    co."ICUSTAY_ID", min("VALUENUM") as MeanBP_min
  from ALINE_COHORT co
  inner join chartevents ce
    on ce."SUBJECT_ID" = co."SUBJECT_ID"
    and ce."CHARTTIME" <= co.vent_starttime
    and ce."CHARTTIME" >= co.vent_starttime - interval '1' day
    and ce."ITEMID" in (456,52,6702,443,220052,220181,225312)
  group by co."ICUSTAY_ID"
)

, uo as
(
  select co."ICUSTAY_ID"
  -- volumes associated with urine output ITEMIDs
  , sum("VALUE") as UrineOutput

  from aline_cohort co
  -- Join to the outputevents table to get urine output
  left join outputevents oe
    on co."SUBJECT_ID" = oe."SUBJECT_ID"
    -- ensure the data occurs during the first day
    and oe."CHARTTIME" between co.vent_starttime - interval '1' day and co.vent_starttime
    and "ITEMID" in
    (
    -- these are the most frequently occurring urine output observations in CareVue
    40055, -- "Urine Out Foley"
    43175, -- "Urine ."
    40069, -- "Urine Out Void"
    40094, -- "Urine Out Condom Cath"
    40715, -- "Urine Out Suprapubic"
    40473, -- "Urine Out IleoConduit"
    40085, -- "Urine Out Incontinent"
    40057, -- "Urine Out Rt Nephrostomy"
    40056, -- "Urine Out Lt Nephrostomy"
    40405, -- "Urine Out Other"
    40428, -- "Urine Out Straight Cath"
    40086,--	Urine Out Incontinent
    40096, -- "Urine Out Ureteral Stent #1"
    40651, -- "Urine Out Ureteral Stent #2"

    -- these are the most frequently occurring urine output observations in Metavision
    226559, -- "Foley"
    226560, -- "Void"
    227510, -- "TF Residual"
    226561, -- "Condom Cath"
    226584, -- "Ileoconduit"
    226563, -- "Suprapubic"
    226564, -- "R Nephrostomy"
    226565, -- "L Nephrostomy"
    226567, --	Straight Cath
    226557, -- "R Ureteral Stent"
    226558  -- "L Ureteral Stent"
    )
  group by co."ICUSTAY_ID"
)

---------
-- GCS --
---------

, gcs_base as
(
  select co."ICUSTAY_ID", l."CHARTTIME"
  , ROW_NUMBER ()
          OVER (PARTITION BY co."ICUSTAY_ID" ORDER BY l."CHARTTIME" ASC) as rn

  -- merge the ITEMIDs so that the pivot applies to both metavision/carevue data
  , max(case when l."ITEMID" in (454,223901) then l."VALUENUM" else null end) as GCSMotor
  , max(case when l."ITEMID" in (723,223900) then l."VALUENUM" else null end) as GCSVerbal
  , max(case when l."ITEMID" in (184,220739) then l."VALUENUM" else null end) as GCSEyes

  -- flag indicating gcs verbal set to 0 due to mechanical ventilation
  , max(case
      -- endotrach/vent is assigned a value of 0, later parsed specially
      when l."ITEMID" = 723 and l."VALUE" = '1.0 ET/Trach' then 1 -- carevue
      when l."ITEMID" = 223900 and l."VALUE" = 'No Response-ETT' then 1 -- metavision
      else 0 end) as EndoTrachFlag

  from aline_cohort co
  inner join chartevents l
    on co."SUBJECT_ID" = l."SUBJECT_ID"
    and l."CHARTTIME" between co.vent_starttime - interval '1' day and co.vent_starttime
    and l."ITEMID" in -- Isolate the desired GCS variables
    (
      -- 198 -- GCS
      -- GCS components, CareVue
      184, 454, 723
      -- GCS components, Metavision
      , 223900, 223901, 220739
    )
  group by co."ICUSTAY_ID", l."CHARTTIME"
)
, gcs as
(
  select b."ICUSTAY_ID"
  -- Calculate GCS, factoring in special case when they are intubated and prev vals
  -- note that the coalesce are used to implement the following if:
  --  if current value exists, use it
  --  if previous value exists, use it
  --  otherwise, default to normal
  , min(case
      -- replace GCS during sedation with 15
      when b.GCSVerbal = 0
        then 15
      when b.GCSVerbal is null and b2.GCSVerbal = 0
        then 15
      -- if previously they were intub, but they aren't now, do not use previous GCS values
      when b2.GCSVerbal = 0
        then
            coalesce(b.GCSMotor,6)
          + coalesce(b.GCSVerbal,5)
          + coalesce(b.GCSEyes,4)
      -- otherwise, add up score normally, imputing previous value if none available at current time
      else
            coalesce(b.GCSMotor,coalesce(b2.GCSMotor,6))
          + coalesce(b.GCSVerbal,coalesce(b2.GCSVerbal,5))
          + coalesce(b.GCSEyes,coalesce(b2.GCSEyes,4))
      end) as MinGCS

  from gcs_base b
  -- join to itself within 6 hours to get previous value
  left join gcs_base b2
    on b."ICUSTAY_ID" = b2."ICUSTAY_ID" and b."rn" = b2."rn"+1 and b2."CHARTTIME" > b."CHARTTIME" - interval '6' hour
  group by b."ICUSTAY_ID"
)

-- Aggregate the components for the score
, scorecomp as
(
select co."ICUSTAY_ID"
  , v.MeanBP_Min

  -- by the cohort definition, patients are never on vasopressors
  , 0 as rate_norepinephrine
  , 0 as rate_epinephrine
  , 0 as rate_dopamine
  , 0 as rate_dobutamine

  , l.Creatinine_Max
  , l.Bilirubin_Max
  , l.Platelet_Min

  , pf.PaO2FiO2
  , uo.UrineOutput
  , gcs.MinGCS

from aline_cohort co
left join stg_pafi pf
 on co."ICUSTAY_ID" = pf."ICUSTAY_ID"
 and pf.rn = 1
left join vitals v
  on co."ICUSTAY_ID" = v."ICUSTAY_ID"
left join labs l
  on co."ICUSTAY_ID" = l."ICUSTAY_ID"
left join uo
  on co."ICUSTAY_ID" = uo."ICUSTAY_ID"
left join gcs gcs
  on co."ICUSTAY_ID" = gcs."ICUSTAY_ID"
)
, scorecalc as
(
  -- Calculate the final score
  -- note that if the underlying data is missing, the component is null
  -- eventually these are treated as 0 (normal), but knowing when data is missing is useful for debugging
  select "ICUSTAY_ID"
  -- Respiration
  , case
      -- patient is never ventilated
      -- when PaO2FiO2_vent_min   < 100 then 4
      -- when PaO2FiO2_vent_min   < 200 then 3
      when PaO2FiO2 < 300 then 2
      when PaO2FiO2 < 400 then 1
      when PaO2FiO2 is null then null
      else 0
    end as respiration

  -- Coagulation
  , case
      when platelet_min < 20  then 4
      when platelet_min < 50  then 3
      when platelet_min < 100 then 2
      when platelet_min < 150 then 1
      when platelet_min is null then null
      else 0
    end as coagulation

  -- Liver
  , case
      -- Bilirubin checks in mg/dL
        when Bilirubin_Max >= 12.0 then 4
        when Bilirubin_Max >= 6.0  then 3
        when Bilirubin_Max >= 2.0  then 2
        when Bilirubin_Max >= 1.2  then 1
        when Bilirubin_Max is null then null
        else 0
      end as liver

  -- Cardiovascular
  , case
      -- when rate_dopamine > 15 or rate_epinephrine >  0.1 or rate_norepinephrine >  0.1 then 4
      -- when rate_dopamine >  5 or rate_epinephrine <= 0.1 or rate_norepinephrine <= 0.1 then 3
      -- when rate_dopamine >  0 or rate_dobutamine > 0 then 2
      when MeanBP_Min < 70 then 1
      when MeanBP_Min is null then null
      else 0
    end as cardiovascular

  -- Neurological failure (GCS)
  , case
      when (MinGCS >= 13 and MinGCS <= 14) then 1
      when (MinGCS >= 10 and MinGCS <= 12) then 2
      when (MinGCS >=  6 and MinGCS <=  9) then 3
      when  MinGCS <   6 then 4
      when  MinGCS is null then null
  else 0 end
    as cns

  -- Renal failure - high creatinine or low urine output
  , case
    when (Creatinine_Max >= 5.0) then 4
    when  UrineOutput < 200 then 4
    when (Creatinine_Max >= 3.5 and Creatinine_Max < 5.0) then 3
    when  UrineOutput < 500 then 3
    when (Creatinine_Max >= 2.0 and Creatinine_Max < 3.5) then 2
    when (Creatinine_Max >= 1.2 and Creatinine_Max < 2.0) then 1
    when coalesce(UrineOutput, Creatinine_Max) is null then null
  else 0 end
    as renal
  from scorecomp
)
select co."SUBJECT_ID", co."HADM_ID", co."ICUSTAY_ID"
  -- Combine all the scores to get SOFA
  -- Impute 0 if the score is missing
  , coalesce(respiration,0)
  + coalesce(coagulation,0)
  + coalesce(liver,0)
  + coalesce(cardiovascular,0)
  + coalesce(cns,0)
  + coalesce(renal,0)
  as SOFA
, respiration
, coagulation
, liver
, cardiovascular
, cns
, renal
from aline_cohort co
left join scorecalc s
  on co."ICUSTAY_ID" = s."ICUSTAY_ID"
order by co."ICUSTAY_ID";
