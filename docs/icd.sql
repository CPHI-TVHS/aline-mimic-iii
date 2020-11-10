-- Extract data which is based on ICD-9 codes

CREATE  VIEW ALINE_ICD AS
select
  co."HADM_ID"
  , max(case when "ICD9_CODE" in
  (  '03642','07422','09320','09321','09322','09323','09324','09884'
    ,'11281','11504','11514','11594'
    ,' 3911',' 4210',' 4211',' 4219'
    ,'42490','42491','42499'
  ) then 1 else 0 end) as endocarditis

  -- chf
  , max(case when "ICD9_CODE" in
  (  '39891','40201','40291','40491','40413'
    ,'40493','4280 ','4281 ','42820','42821'
    ,'42822','42823','42830','42831','42832'
    ,'42833','42840','42841','42842','42843'
    ,'4289 ','428  ','4282 ','4283 ','4284 '
  ) then 1 else 0 end) as chf

  -- atrial fibrilliation or atrial flutter
  , max(case when "ICD9_CODE" like '4273%' then 1 else 0 end) as afib

  -- renal
  , max(case when "ICD9_CODE" like '585%' then 1 else 0 end) as renal

  -- liver
  , max(case when "ICD9_CODE" like '571%' then 1 else 0 end) as liver

  -- copd
  , max(case when "ICD9_CODE" in
  (  '4660 ','490  ','4910 ','4911 ','49120'
    ,'49121','4918 ','4919 ','4920 ','4928 '
    ,'494  ','4940 ','4941 ','496  ') then 1 else 0 end) as copd

  -- coronary artery disease
  , max(case when "ICD9_CODE" like '414%' then 1 else 0 end) as cad

  -- stroke
  , max(case when "ICD9_CODE" like '430%'
      or "ICD9_CODE" like '431%'
      or "ICD9_CODE" like '432%'
      or "ICD9_CODE" like '433%'
      or "ICD9_CODE" like '434%'
       then 1 else 0 end) as stroke

  -- malignancy, includes remissions
  , max(case when "ICD9_CODE" between '140' and '239' then 1 else 0 end) as malignancy

  -- resp failure
  , max(case when "ICD9_CODE" like '518%' then 1 else 0 end) as respfail

  -- ARDS
  , max(case when "ICD9_CODE" = '51882' or "ICD9_CODE" = '5185 ' then 1 else 0 end) as ards

  -- pneumonia
  , max(case when "ICD9_CODE" between '486' and '48881'
      or "ICD9_CODE" between '480' and '48099'
      or "ICD9_CODE" between '482' and '48299'
      or "ICD9_CODE" between '506' and '5078'
        then 1 else 0 end) as pneumonia
from aline_cohort co
left join diagnoses_icd icd
  on co."HADM_ID" = icd."HADM_ID"
group by co."HADM_ID"
order by co."HADM_ID";
