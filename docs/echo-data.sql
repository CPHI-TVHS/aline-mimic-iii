SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED ;

drop table if exists ALINE_ECHODATA;
/*
CREATE TABLE ALINE_ECHODATA
select 	subject_id
		, hadm_id
		, icustay_id
		, row_id
		, chartdate
		, charttime
		, indication
		-- height in inches
		, height as height_first
		-- weight in lbs
		, weight as weight_first
		, case
			when weight is not null and height is not null
				then 703.0 * (weight / (height*height))
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
from (
*/
	select co.subject_id
		, co.hadm_id
        , co.icustay_id
		, ne.ROW_ID
		, ne.chartdate
		, case when ne.chartdate is not null then DATE_FORMAT(ne.chartdate, '%Y-%m-%d %H:%i:%s' )  else substring(ne.text, 'Date/Time: [\[\]0-9*-]+ at ([0-9:]+)') end as charttime
		, substring(ne.text, 'Indication: (.*?)\n') as Indication
        
		, case
			when substring(ne.text, 'Height: \(in\) (.*?)\n') like '%*%' then null
			else cast(substring(ne.text, 'Height: \(in\) (.*?)\n') as double)
		end as Height

		, case
			when substring(ne.text, 'Weight \(lb\): (.*?)\n') like '%*%' then null
			else cast(substring(ne.text, 'Weight \(lb\): (.*?)\n') as double)
		end as Weight
        
		, case
			when substring(ne.text, 'BSA \(m2\): (.*?) m2\n') like '%*%' then null
			else cast(substring(ne.text, 'BSA \(m2\): (.*?) m2\n') as double)
			end as BSA -- ends in 'm2'

		, substring(ne.text, 'BP \(mm Hg\): (.*?)\n') as BP -- Sys/Dias

		, case
			when substring(ne.text, 'BP \(mm Hg\): ([0-9]+)/[0-9]+?\n') like '%*%' then null
			else cast(substring(ne.text, 'BP \(mm Hg\): ([0-9]+)/[0-9]+?\n') as double)
			end as BPSys -- first part of fraction

		, case
			when substring(ne.text, 'BP \(mm Hg\): [0-9]+/([0-9]+?)\n') like '%*%' then null
			else cast(substring(ne.text, 'BP \(mm Hg\): [0-9]+/([0-9]+?)\n') as double)
			end as BPDias -- second part of fraction

	  , case
		  when substring(ne.text, 'HR \(bpm\): ([0-9]+?)\n') like '%*%'
			then null
		  else cast(substring(ne.text, 'HR \(bpm\): ([0-9]+?)\n') as double)
		end as HR

	  , substring(ne.text, 'Status: (.*?)\n') as Status
	  , substring(ne.text, 'Test: (.*?)\n') as Test
	  , substring(ne.text, 'Doppler: (.*?)\n') as Doppler
	  , substring(ne.text, 'Contrast: (.*?)\n') as Contrast
	  , substring(ne.text, 'Technical Quality: (.*?)\n') as TechnicalQuality

	  , ROW_NUMBER() over (
		PARTITION BY co.icustay_id 
		ORDER BY ne.chartdate
      ) as rn

      
	from ALINE_COHORT co
		left join noteevents ne
			on co.hadm_id = ne.hadm_id
			and ne.category = 'Echo'
			and ne.chartdate <= co.vent_starttime
			and ne.chartdate >= (co.vent_starttime - interval '7' day)
	limit 10;
/*
) as ed
where rn = 1;
*/


SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ ;