

CREATE  VIEW ALINE_BMI as

with ce_wt as
(
    SELECT
      co."ICUSTAY_ID"
      -- we take the median value from their stay
      -- TODO: eliminate obvious outliers if there is a reasonable weight
      -- (e.g. weight of 180kg and 90kg would remove 180kg instead of taking the median)
      , percentile_cont(0.5) WITHIN GROUP (ORDER BY "VALUENUM") as Weight_Admit
    FROM aline_cohort co
    inner join chartevents c
        on c."SUBJECT_ID" = co."SUBJECT_ID"
        and c."CHARTTIME" between co.vent_starttime - interval '1' day and co.vent_starttime
    WHERE c."VALUENUM" IS NOT NULL
    AND c."ITEMID" in (762,226512) -- Admit Wt
    AND c."VALUENUM" != 0
    group by co."ICUSTAY_ID"
)
, dwt as
(
    SELECT
      co."ICUSTAY_ID"
      , percentile_cont(0.5) WITHIN GROUP (ORDER BY "VALUENUM") as Weight_Daily
    FROM aline_cohort co
    inner join chartevents c
        on c."SUBJECT_ID" = co."SUBJECT_ID"
        and c."CHARTTIME" between co.vent_starttime - interval '1' day and co.vent_starttime
    WHERE c."VALUENUM" IS NOT NULL
    AND c."ITEMID" in (763,224639) -- Daily Weight
    AND c."VALUENUM" != 0
    group by co."ICUSTAY_ID"
)
, ce_ht0 as
(
    SELECT
      co."ICUSTAY_ID"
      , case
        -- convert inches to centimetres
          when "ITEMID" in (920, 1394, 4187, 3486)
              then "VALUENUM" * 2.54
            else "VALUENUM"
        end as Height
    FROM aline_cohort co
    inner join chartevents c
        on c."SUBJECT_ID" = co."SUBJECT_ID"
        and c."CHARTTIME" <= co."OUTTIME"
    WHERE c."VALUENUM" IS NOT NULL
    AND c."ITEMID" in (226730,920, 1394, 4187, 3486,3485,4188) -- height
    AND c."VALUENUM" != 0
)
, ce_ht as
(
    SELECT
        "ICUSTAY_ID"
        -- extract the median height from the chart to add robustness against outliers
        , percentile_cont(0.5) WITHIN GROUP (ORDER BY height) as Height_chart
    from ce_ht0
    group by "ICUSTAY_ID"
)
, echo as
(
    select "ICUSTAY_ID"
        , 2.54*height_first as height_echo
        , 0.453592*weight_first as weight_echo
    from aline_echodata ec
)
, bmi as
(
select
    co."ICUSTAY_ID"
    -- weight in kg
    , round(cast(
          coalesce(ce_wt.Weight_Admit, dwt.Weight_Daily, ec.weight_echo)
        as numeric), 2)
    as Weight

    -- height in metres
    , coalesce(ce_ht.Height_chart, ec.height_echo)/100.0 as Height

    -- components
    , ce_ht.Height_chart
    , ce_wt.Weight_Admit
    , dwt.Weight_Daily
    , ec.Height_echo
    , ec.Weight_echo

from aline_cohort co

-- admission weight
left join ce_wt
    on co."ICUSTAY_ID" = ce_wt."ICUSTAY_ID"

-- daily weights
left join dwt
    on co."ICUSTAY_ID" = dwt."ICUSTAY_ID"

-- height
left join ce_ht
    on co."ICUSTAY_ID" = ce_ht."ICUSTAY_ID"

-- echo data
left join echo ec
    on co."ICUSTAY_ID" = ec."ICUSTAY_ID"
)
select
    "ICUSTAY_ID"
    , case
        when weight is not null and height is not null
            then (weight / (height*height))
        else null
    end as BMI
    , height
    , weight

    -- components
    , Height_chart
    , Weight_Admit
    , Weight_Daily
    , Height_echo
    , Weight_echo
from bmi
order by "ICUSTAY_ID";
