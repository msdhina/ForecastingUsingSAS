/*  SAS Forecasting - Sample Time series */
/*  Created by - Dhinakar M              */
/*  Date - 28 Jan 2022					 */


/*  ----------------------------- DATA INFO ------------------------------------- */
/*Total rows: 204 Total columns: 2 */

%web_drop_table(WORK.Sample);
FILENAME REFFILE '/home/u60762623/Forecast Sample/Train.csv';
PROC IMPORT DATAFILE=REFFILE
	DBMS=CSV
	OUT=WORK.Sample;
	GETNAMES=YES;
	DATAROW=2;
RUN;
%web_open_table(WORK.Sample);


/*  ----------------------------- TIME SERIES EXPLORATION ---------------------------- */
/*  Time Series Trend  */
/*
DATA WORK.SAMPLE_2000;
    SET WORK.SAMPLE;
    IF year(date) > 2005 THEN OUTPUT;
RUN;
ods graphics / reset width=25 in height=5in imagemap;
proc sgplot data=WORK.SAMPLE;
	vline date / response=value;
	xaxis valuesrotate=vertical;
	yaxis grid;
run;
ods graphics / reset;
*/

/* Decomposition */
ods noproctitle;
ods graphics / imagemap=on;
proc sort data=WORK.SAMPLE out=Work.preProcessedData;
	by date; run;
proc timeseries data=Work.preProcessedData seasonality=12 plots=(series 
		histogram corr decomp) print=(descstats seasons decomp);
	id date interval=month;
	var value / accumulate=none transform=none dif=0 sdif=0;
	decomp / mode=multoradd;
run;


/* Unit root test analysis - ADF */
proc arima data=Work.preProcessedData plots=none;
	ods select StationarityTests;
	identify var=value(1) stationarity=(adf=2);
	run;

/*  ----------------------------- MODELLING ------------------------------------- */
/* ARIMA */

proc arima data=Work.preProcessedData plots
     (only)=(series(corr crosscorr) residual(acf corr hist iacf normal 
		pacf qq smooth wn) forecast(forecast 
		) ) out=work.forecast_values;
	identify var=value(1);
	estimate p=(1) q=(1)(12) method=ML;
	forecast lead=24 back=0 alpha=0.05 id=date interval=month out = result;
run;

proc export data=work.result
    outfile="/home/u60762623/Forecast Sample/forecast_values.csv"
    dbms=csv;
run;


/* Minimum Information Criteria Matrix */

PROC ARIMA DATA= Work.preProcessedData;
IDENTIFY VAR = value(1,12) MINIC;
RUN;


/* Iterating till Min Info */

%Macro top_models;

%do p = 0  %to 2 ;
%do q = 0 % to 2 ;

PROC ARIMA DATA= Work.preProcessedData ;
IDENTIFY VAR = value(1,12)  ;
ESTIMATE P = &p. Q =&q.  OUTSTAT= stats_&p._&q. ;
Forecast lead=12 interval = month id = date 
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
set %do p = 0  %to 2 ;
%do q = 0 % to 2 ;
stats_&p._&q. 
%end;
%end;;
Run;

Data final_results ;
set %do p = 0  %to 2 ;
%do q = 0 % to 2 ;
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
Data Mape;
set final_results;
Ind_Mape = abs(value - forecast)/ value;
Run;


Proc Sql;
create table mape as select p, q, mean(ind_mape) as mape from mape
group by p, q
order by mape ;
quit;



/* Winter's */

proc esm data=Work.preProcessedData back=0 lead=24 seasonality=12 plot=(corr 
		errors modelforecasts) outfor=work.result_winters;
	id date interval=month;
	forecast value / alpha=0.05 model=winters transform=none;
run;
proc export data=work.result_winters
    outfile="/home/u60762623/Forecast Sample/forecast_values_winters.csv"
    dbms=csv;
run;

/* Random Walk */

ods noproctitle;
ods graphics / imagemap=on;

proc arima data=Work.preProcessedData plots
    (only)=(series(corr crosscorr) residual(corr normal) 
		forecast(forecast 
		) ) out=work.results_randowmwalk;
	identify var=value (1 1 12);
	estimate noint method=CLS outest=work.RandowWalkEst;
	forecast lead=24 back=0 alpha=0.05 id=date interval=month;
	run;

proc export data=work.results_randowmwalk
    outfile="/home/u60762623/Forecast Sample/forecast_values_RandomWalk.csv"
    dbms=csv;
run;


/* Moving Average */

proc arima data=Work.preProcessedData plots
     (only)=(series(corr crosscorr) residual(corr normal) 
		forecast(forecast) ) out=work.results_movingavg;
	identify var=value;
	estimate q=(1 2 3 4 5 6 7 8 9 10 11 12) ma=(0.08333333333333333 
		0.08333333333333333 0.08333333333333333 0.08333333333333333 
		0.08333333333333333 0.08333333333333333 0.08333333333333333 
		0.08333333333333333 0.08333333333333333 0.08333333333333333 
		0.08333333333333333 0.08333333333333333) noint method=CLS;
	forecast lead=24 back=0 alpha=0.05 id=date interval=month;
	run;
	
proc export data=work.results_movingavg
    outfile="/home/u60762623/Forecast Sample/forecast_values_MovingAverage.csv"
    dbms=csv;
run;

