/* TODO: Clamping bounds */ 
%let lower_bound = 0; 
%let upper_bound = 120; 
 
/* privacy parameters */ 
%let epsilon = 0.2; 
 
/* Loading the dataset  */ 
proc import  
	datafile = "/home/u61808647/NRF/data/simulated_data.csv" 
	dbms=csv 
	out=survey_data replace; 
run; 
 
/* group by followed by summing */ 
proc sql; 
	create table survey_table as 
    select SSIC, firm_type, sum(exp_total_rnd) as berd, count(*) as firms_count 
    from survey_data 
    group by SSIC, firm_type; 
quit; 
 
/* printing the grouped by table */ 
proc print data=survey_table; 
 
/* run; */ 
/* 	#RAND('NORMal', 0, &sd.); */ 
/* adding noise to the values */ 
data private_survey_data; 
	set survey_table; 
	noisy_berd = berd +  RAND('LAPLace', 0, 1/&epsilon.); 
	noisy_firms_count = firms_count +  RAND('LAPLace', 0, 1/&epsilon.); 
	error_berd = noisy_berd - berd;  
	error_firms_count = noisy_firms_count - firms_count;  
run; 
 
proc print data=private_survey_data; 
run; 
 
/* 	gauss_noisy_berd = berd +  RAND('NORMal', 0, 1/&epsilon.);  */ 