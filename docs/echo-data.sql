-- This code extracts structured data from echocardiographies using regex

-- Echos are examined between the start of ventilation and 7 days prior

-- It can be rejoined to the original notes in NOTEVENTS using ROW_ID
-- Just keep in mind that ROW_ID will differ across versions of MIMIC-III.

DROP MATERIALIZED VIEW IF EXISTS ALINE_ECHODATA CASCADE;
CREATE MATERIALIZED VIEW ALINE_ECHODATA AS
with ed as
(
select
  co."SUBJECT_ID", co."HADM_ID", co."ICUSTAY_ID"
  , ne."ROW_ID"
  , ne."CHARTDATE"

  -- charttime is always null for echoes..
  -- however, the time is available in the echo text, e.g.:
  -- , substring(ne.text, 'Date/Time: [\[\]0-9*-]+ at ([0-9:]+)') as TIMESTAMP
  -- we can therefore impute it and re-create charttime
  , cast(to_timestamp( (to_char( ne."CHARTDATE", 'DD-MM-YYYY' ) || substring(ne."TEXT", 'Date/Time: [\[\]0-9*-]+ at ([0-9:]+)')),
            'DD-MM-YYYYHH24:MI') as timestamp without time zone)
    as charttime

  -- explanation of below substring:
  --  'Indication: ' - matched verbatim
  --  (.*?) - match any character
  --  \n - the end of the line
  -- substring only returns the item in ()s
  -- note: the '?' makes it non-greedy. if you exclude it, it matches until it reaches the *last* \n

  , substring(ne."TEXT", 'Indication: (.*?)\n') as Indication

  -- sometimes numeric values contain de-id text, e.g. [** Numeric Identifier **]
  -- this removes that text
  , case
      when substring(ne."TEXT", 'Height: \(in\) (.*?)\n') like '%*%'
        then null
      else cast(substring(ne."TEXT", 'Height: \(in\) (.*?)\n') as numeric)
    end as Height

  , case
      when substring(ne."TEXT", 'Weight \(lb\): (.*?)\n') like '%*%'
        then null
      else cast(substring(ne."TEXT", 'Weight \(lb\): (.*?)\n') as numeric)
    end as Weight

  , case
      when substring(ne."TEXT", 'BSA \(m2\): (.*?) m2\n') like '%*%'
        then null
      else cast(substring(ne."TEXT", 'BSA \(m2\): (.*?) m2\n') as numeric)
    end as BSA -- ends in 'm2'

  , substring(ne."TEXT", 'BP \(mm Hg\): (.*?)\n') as BP -- Sys/Dias

  , case
      when substring(ne."TEXT", 'BP \(mm Hg\): ([0-9]+)/[0-9]+?\n') like '%*%'
        then null
      else cast(substring(ne."TEXT", 'BP \(mm Hg\): ([0-9]+)/[0-9]+?\n') as numeric)
    end as BPSys -- first part of fraction

  , case
      when substring(ne."TEXT", 'BP \(mm Hg\): [0-9]+/([0-9]+?)\n') like '%*%'
        then null
      else cast(substring(ne."TEXT", 'BP \(mm Hg\): [0-9]+/([0-9]+?)\n') as numeric)
    end as BPDias -- second part of fraction

  , case
      when substring(ne."TEXT", 'HR \(bpm\): ([0-9]+?)\n') like '%*%'
        then null
      else cast(substring(ne."TEXT", 'HR \(bpm\): ([0-9]+?)\n') as numeric)
    end as HR

  , substring(ne."TEXT", 'Status: (.*?)\n') as Status
  , substring(ne."TEXT", 'Test: (.*?)\n') as Test
  , substring(ne."TEXT", 'Doppler: (.*?)\n') as Doppler
  , substring(ne."TEXT", 'Contrast: (.*?)\n') as Contrast
  , substring(ne."TEXT", 'Technical Quality: (.*?)\n') as TechnicalQuality

  , ROW_NUMBER() over (PARTITION BY co."ICUSTAY_ID" ORDER BY cast(to_timestamp( (to_char( ne."CHARTDATE", 'DD-MM-YYYY' ) || substring(ne."TEXT", 'Date/Time: [\[\]0-9*-]+ at ([0-9:]+)')),
            'DD-MM-YYYYHH24:MI') as timestamp without time zone)) as rn
from ALINE_COHORT co
left join noteevents ne
  on co."HADM_ID" = ne."HADM_ID"
  and ne."CATEGORY" = 'Echo'
  and ne."CHARTDATE" <= co."vent_starttime"
  and ne."CHARTDATE" >= date_trunc('day', co."vent_starttime" - interval '7' day)
)
select
  "SUBJECT_ID"
, "HADM_ID"
, "ICUSTAY_ID"
, "ROW_ID"
, "CHARTDATE"
, "charttime"
, "indication"
-- height in inches
, "height" as height_first
-- weight in lbs
, "weight" as weight_first
, case
    when "weight" is not null and "height" is not null
        then 703.0 * ("weight" / ("height"*"height"))
    else null
  end as BMI
, bsa as bsa_first
, bp
, bpsys
, bpdias
, hr
, status
, test
, doppler
, contrast
, technicalquality
from ed
where rn = 1;
