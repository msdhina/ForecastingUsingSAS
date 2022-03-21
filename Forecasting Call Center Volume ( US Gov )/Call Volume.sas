LIBNAME CALL '/home/u60762623/Forecast Call Center';

/* Data Import */ 

%web_drop_table(CALL.CURRENT0);
FILENAME REFFILE '/home/u60762623/Forecast Call Center/callcenterdatacurrent.csv' TERMSTR=CR ;
PROC IMPORT DATAFILE=REFFILE 
	DBMS=CSV
	OUT=CALL.CURRENT0;
	GETNAMES=YES;
RUN;
PROC PRINT DATA=CALL.CURRENT0(obs=5) ; RUN;
%web_open_table(CALL.CURRENT0);


%web_drop_table(CALL.HISTORY0);
FILENAME REFFILE '/home/u60762623/Forecast Call Center/callcenterdatahistorical.csv' TERMSTR=CR;
PROC IMPORT DATAFILE=REFFILE
	DBMS=CSV
	OUT=CALL.HISTORY0;
	GETNAMES=YES;
RUN;
PROC PRINT DATA=CALL.HISTORY0(obs=5) ; RUN;
%web_open_table(CALL.HISTORY0);


/*------------------------------------Data Prep---------------------------------------------------------*/
/* Changing Title */


proc sql;

   create table CALL.PREP1 as
   select input(strip(substr(CREATIONDATE, 1,10)),yymmdd10.) format date9. as DATE, 
   OBJECTDESC,
          case
            when TITLE like  'Abandoned Vehicle%'   then 'Abandoned Vehicle'
			when TITLE like  'ZOO%'  then 'Zoo'
			when TITLE like  'Zoning Violation Web%'  then 'Zoning Violation Web'
			when TITLE like  'Zoning Violation%'  then 'Zoning Violation'
			when TITLE like  'ZONING INFORMATION%'  then 'Zoning Information'
			when TITLE like  'ZONING APPEALS%'  then 'Zoning Appeals'
			when TITLE like  'ZIP CODES%'  then 'Zip Codes'
			when TITLE like  'YOUTH EMPLOYMENT%'  then 'Youth Employment'
			when TITLE like  'Yield Sign%'  then 'Yield Sign'
			when TITLE like  'WORKER’S COMPENSATION%'  then 'Worker’S Compensation'
			when TITLE like  'WORK PERMITS, Students%'  then 'Work Permits, Students'
			when TITLE like  'WORK ISSUES, Public%'  then 'Work Issues, Public'
			when TITLE like  'Work Being Done Without Permit, Private Property%'  then 'Work Being Done Without Permit, Private Property'
			when TITLE like  'Work Being Done Without Permit%'  then 'Work Being Done Without Permit'
			when TITLE like  'Wood Chips Inquiry%'  then 'Wood Chips Inquiry'
			when TITLE like  'WOOD CHIPS (Free)%'  then 'Wood Chips (Free)'
			when TITLE like  'WISCONSIN, State of%'  then 'Wisconsin, State Of'
			when TITLE like  'WISCONSIN STATE FAIR PARK%'  then 'Wisconsin State Fair Park'
			when TITLE like  'WISCONSIN HUMANE SOCIETY%'  then 'Wisconsin Humane Society'
			when TITLE like  'WISCONSIN GAS COMPANY%'  then 'Wisconsin Gas Company'
			when TITLE like  'WISCONSIN ELECTRIC POWER%'  then 'Wisconsin Electric Power'
			when TITLE like  'WISCONSIN DEPT. OF TRANSPORTATION%'  then 'Wisconsin Dept. Of Transportation'
			when TITLE like  'WISCONSIN DEPT. OF CHILDREN & FAMILIES%'  then 'Wisconsin Dept. Of Children & Families'
			when TITLE like  'WISCONSIN CENTER DISTRICT%'  then 'Wisconsin Center District'
			when TITLE like  'Wires Down%'  then 'Wires Down'
			when TITLE like  'WINTER PARKING REGULATIONS%'  then 'Winter Parking Regulations'
			when TITLE like  'Windows Broken or Missing%'  then 'Windows Broken Or Missing'
			when TITLE like  'Windows Broken Or Missing%'  then 'Windows Broken Or Missing'
			when TITLE like  'WILSON RECREATION CENTER%'  then 'Wilson Recreation Center'
			when TITLE like  'WILDLIFE ANIMALS, Injured	(Within Milw Cnty)%'  then 'Wildlife Animals, Injured	(Within Milw Cnty)'
			when TITLE like  'WILDLIFE ANIMALS, INJURED%'  then 'Wildlife Animals, Injured'
			when TITLE like  'WILD ANIMALS%'  then 'Wild Animals'
			when TITLE like  'WIC PROGRAM   (Women, Infants & Children)%'  then 'Wic Program   (Women, Infants & Children)'
			when TITLE like  'WHITEFISH BAY, Village%'  then 'Whitefish Bay, Village'
			when TITLE like  'What%'  then 'Misc'
			when TITLE like  'Where%'  then 'Misc'
			when TITLE like  'Vehicle%'  then 'Vehicle'
			when TITLE like  'Traffic%'  then 'Traffic'
			when TITLE like  'Street%'  then 'Street'
			when TITLE like  '%Debris%%'  then 'Debris'
			when TITLE like  'Snow%'  then 'Snow'
			when TITLE like  'Sidewalk%'  then 'Sidewalk'
			when TITLE like  'Recycling%'  then 'Recycling'
			when TITLE like  'Public right of way%'  then 'Public right of way'
			when TITLE like  'Pothole%'  then 'Pothole'
			when TITLE like  'Missed%'  then 'Missed'
			when TITLE like  'Leaf%'  then 'Leaf'
			when TITLE like  'Graffiti%'  then 'Graffiti'
			when TITLE like  'Complaint Weeds%'  then 'Complaint Weeds'
			else TITLE
          end as TITLE
      from CALL.HISTORY0;



/* Add ADDRESS_HOME & ADDRESS_ZIP*/
data call.PREP2;
set call.PREP1;
ADDRESS_ZIP=scan(OBJECTDESC,-1,',');
ADDRESS_HOME=scan(OBJECTDESC,1,',');
ZIP_CHECK=index(ADDRESS_ZIP,'-');
if ZIP_CHECK <7 then ADDRESS_ZIP="NA" ;
run;

data call.PREP2;
set call.PREP2;
if length(OBJECTDESC)=0 then ADDRESS_ZIP='NA';
if length(OBJECTDESC)=0 then ADDRESS_HOME='NA';
ADDRESS_ZIP_CODE = scan(ADDRESS_ZIP,1,'-');
ADDRESS_ZIP_PO = scan(ADDRESS_ZIP,2,'-');
drop ZIP_CHECK;
run;

/* Top Zip code by count of dates >1000 */
proc sql;
create TABLE CALL.PREP2_ZIP_GE_1000 as
select ADDRESS_ZIP_CODE, count(*) as COUNT from
CALL.PREP2
group by ADDRESS_ZIP_CODE;
run;

data call.PREP2_ZIP_GE_1000;
set call.PREP2_ZIP_GE_1000;
if (anyalpha(ADDRESS_ZIP_CODE)) then delete;
where count>1000;
run;



/* ----------------------- DYNAMIC DATASETS + MODELLING ----------------------------------------------------- */

proc sort data=call.PREP2_ZIP_GE_1000 out=call.PREP2_ZIP_GE_1000 (keep=ADDRESS_ZIP_CODE)
nodupkey;
by ADDRESS_ZIP_CODE;
run;

proc SQL;
create table call.prep3 as
select ADDRESS_ZIP_CODE, DATE, count(*) as VOLUME from call.prep2
group by ADDRESS_ZIP_CODE, DATE;
run;


data _null_;
set call.PREP2_ZIP_GE_1000;
call execute('ZIP_' !! compress(ADDRESS_ZIP_CODE) !! '; set call.prep3;
	 where ADDRESS_ZIP_CODE="'!! ADDRESS_ZIP_CODE !!'"; run;');
run;

%macro datasplit;
proc sql noprint;
select distinct address_zip_code into: ZIP1- from call.prep3;
quit;
proc sql noprint;
select count(distinct address_zip_code) into: n from call.prep3;
quit;
%do i = 1 %to &n.;
	data CALL.PREP_ZIP;
	set call.prep3;
	where address_zip_code="&&ZIP&i";
	run;
%end;
%mend;

%datasplit;



proc sort data=work.sales out=work.unique (keep=Country)
nodupkey;
by country;
run;


















/*---------------------------------------------------------------------------------------------*/
/* Agg to Date, volume */

PROC SQL  ;
CREATE TABLE  CALL.HISTORY2_FULL  AS 
select date,count(*) as VOLUME  from call.history1 
group by date;
run;

PROC SQL  ;
CREATE TABLE  CALL.HISTORY2  AS 
select date,count(*) as VOLUME  from call.history1 
where date between '01Jan2015'd and '31JUL2020'd
group by date;
run;

/*---------------------------------------------------------------------------------------------*/
/* Handle Missing Values */

proc timeseries data=call.history2_FULL out= call.history3_FULL;
id date interval=day setmiss=PREVIOUS;
var volume;
run;

proc timeseries data=call.history2 out= call.history3;
id date interval=day setmiss=PREVIOUS;
var volume;
run;

proc export data=call.history3
    outfile="/home/u60762623/Forecast Call Center/HISTORY3.csv"
    dbms=csv;
run;



/*---------------------------------------------------------------------------------------------*/
/* ARIMA */

%Macro top_models;

%do p = 0  %to 3;
%do q = 0  %to 3 ;

PROC ARIMA DATA= call.history3 ;
IDENTIFY VAR = VOLUME(1) ;
ESTIMATE P = &p. Q =&q.  OUTSTAT= stats_&p._&q. ;
Forecast lead=35 interval = day id = date 
out = result_&p._&q.;
RUN;

data stats_&p._&q.;
set   stats_&p._&q.;
p = &p.;
q = &q.;
Run;

data result_&p._&q.;
set   result_&p._&q.;
p = &p.;
q = &q.;
Run;

%end;
%end;

Data final_stats ;
set %do p = 0  %to 3 ;
%do q = 0 % to 3 ;
stats_&p._&q. 
%end;
%end;;
Run;

Data final_results ;
set %do p = 0  %to 3 ;
%do q = 0 % to 3 ;
result_&p._&q.
%end;
%end;;
Run;

%Mend;
%top_models;

/* Then to calculate the mean of AIC and SBC */

proc sql;
create table final_stats_1  as select p,q, sum(_VALUE_)/2 as mean_aic_sbc from final_stats
where _STAT_ in ('AIC','SBC')
group by p,q
order by mean_aic_sbc;
quit;

proc sql outobs=5;
create table validation  as select * from final_stats_1 order by mean_aic_sbc;
quit;


/* MAPE  =  Abs(Actual – Predicted) / Actual *100  */
Data Mape1;
set final_results;
Ind_Mape = abs(VOLUME - forecast)/ VOLUME;
Run;


Proc Sql;
CREATE TABLE  FINAL_MAPE as
select p, q, mean(ind_mape) as mape from mape1
group by p, q
order by mape ;
quit;


/*---------------------------------------------------------------------------------------------*/
/* ESM */
proc esm data=CALL.HISTORY3 back=0 lead=35 seasonality=365 plot=(corr 
		errors modelforecasts);
	id DATE interval=day;
	forecast VOLUME / alpha=0.05 model=winters transform=none;
run;


/****************************   WEEKLY FORECAST  ************************************/

/* Day to weeklevel data roll up*/
data CALL.HISTORYW_FULL;
set CALL.HISTORY3_FULL;
DATE_WEEK_START = intnx('week',DATE,0,'b');
DATE_WEEK_END = intnx('week',DATE,0,'e');
format DATE_WEEK_START DATE_WEEK_END yymmdd10.;
run;

PROC SQL  ;
CREATE TABLE  CALL.HISTORYWK_FULL  AS
SELECT DATE_WEEK_START,SUM(VOLUME) as VOLUME  from CALL.HISTORYW_FULL 
group by DATE_WEEK_START;
run;


/* Day to weeklevel data roll up*/
data CALL.HISTORYW;
set CALL.HISTORY3;
DATE_WEEK_START = intnx('week',DATE,0,'b');
DATE_WEEK_END = intnx('week',DATE,0,'e');
format DATE_WEEK_START DATE_WEEK_END yymmdd10.;
run;

PROC SQL  ;
CREATE TABLE  CALL.HISTORYWK  AS
SELECT DATE_WEEK_START,SUM(VOLUME) as VOLUME  from CALL.HISTORYW 
group by DATE_WEEK_START;
run;

/* ARIMA */
proc arima data=CALL.HISTORYWK plots=all out=CALL.WEEK_ARIMA;;
	identify var=VOLUME;
	estimate p=(1 1) (52) method=ML;
	forecast lead=35 back=0 alpha=0.05 id=DATE_WEEK_START interval=week;
	run;
quit;


/* ESM */
proc esm data=CALL.HISTORYWK  back=0 lead=35 seasonality=52 plot=(corr 
		errors modelforecasts) outfor=CALL.WEEK_ESM;
	id DATE_WEEK_START interval=week;
	forecast VOLUME / alpha=0.05 model=winters transform=none;
run;



/* MVG AVG */

proc arima data=CALL.HISTORYWK plots=all out=CALL.WEEK_MVG;
	identify var=VOLUME;
	estimate q=(1 2 3 4 5 6 7) ma=(0.14285714285714285 0.14285714285714285 
		0.14285714285714285 0.14285714285714285 0.14285714285714285 
		0.14285714285714285 0.14285714285714285) noint method=CLS;
	forecast lead=35 back=0 alpha=0.05 id=DATE_WEEK_START interval=week;
	run;
quit;


/* RAND WALK */

proc arima data=CALL.HISTORYWK plots=all out=CALL.WEEK_RAND;
	identify var=VOLUME (1 52);
	estimate noint method=CLS;
	forecast lead=35 back=0 alpha=0.05 id=DATE_WEEK_START interval=week;
	run;
quit;


proc export data=call.week_rand outfile="/home/u60762623/Forecast Call Center/call_week_rand.csv" REPLACE dbms=csv; run;
proc export data=call.week_mvg outfile="/home/u60762623/Forecast Call Center/call_week_mvg.csv" REPLACE dbms=csv; run;
proc export data=call.week_esm outfile="/home/u60762623/Forecast Call Center/call_week_esm.csv" REPLACE dbms=csv; run;
proc export data=call.week_arima outfile="/home/u60762623/Forecast Call Center/call_week_arima.csv"  REPLACE dbms=csv; run;
proc export data=call.HISTORYWK outfile="/home/u60762623/Forecast Call Center/Call_week_actual.csv" REPLACE dbms=csv; run;

	
