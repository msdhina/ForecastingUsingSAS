LIBNAME STOCK '/home/u60762623/Forecast Stock Price';

/* Data Import */ 

FILENAME REFFILE '/home/u60762623/Forecast Stock Price/Stock.csv';
PROC IMPORT DATAFILE=REFFILE
	DBMS=CSV
	OUT=STOCK.TRAIN
	replace;
	GETNAMES=YES;
	guessingrows=100000;
	
RUN;
PROC CONTENTS DATA=STOCK.TRAIN; RUN;
%web_open_table(STOCK.TRAIN);

/****************************************************************************************/
/* Add variance  diff col*/ 

/* data stock.train1; */
/* set stock.train; */
/* Vol_Close=Volume/close; */
/* High_low=High-low; */
/* run; */
/*  */


/****************************************************************************************/
/* Time Series Exploration*/ 

ods noproctitle;
ods graphics / imagemap=on;

proc sort data=STOCK.TRAIN out=STOCK.preProcessedData;
	by Date;
run;

proc timeseries data=STOCK.preProcessedData seasonality=7 plots=(series 
		histogram cycles corr decomp);
	id Date interval=day;
	var Close / accumulate=none transform=none dif=0 sdif=0;
	crossvar High / accumulate=none transform=none dif=0 sdif=0;
	crossvar Low / accumulate=none transform=none dif=0 sdif=0;
	crossvar Open / accumulate=none transform=none dif=0 sdif=0;
	crossvar Volume / accumulate=none transform=none dif=0 sdif=0;
	crossvar 'Adj Close'n / accumulate=none transform=none dif=0 sdif=0;
	decomp / mode=multoradd;
run;


/****************************************************************************************/
/*MultiVariate Modeling*/ 


ods noproctitle;
ods graphics / imagemap=on;



proc sort data=STOCK.PREPROCESSEDDATA out=STOCK.preProcessedData;
	by Date;
run;

data STOCK.PREPROCESSEDDATA_SPL;
  set STOCK.PREPROCESSEDDATA;
  if date > '31JUL2020'd then close=.; 
  *drop2019 = ('01JAN2020'd <= date <= '31DEC2020'd);
run;


proc arima data=STOCK.PREPROCESSEDDATA_SPL plots
     (only)=(residual(corr normal) forecast(forecast) );
	identify var=CLOSE crosscorr=( variancelast7  vol_close vix IntRate LastMonthPER) noprint;
	estimate p=(1) (1) input=(  variancelast7  vol_close vix IntRate LastMonthPER) method=ML;
	forecast lead=90 back=0 alpha=0.05 id=Date interval=day out=STOCK.ARIMAX;
	run;
quit;

proc export data=STOCK.ARIMAX REPLACE 
    outfile="/home/u60762623/Forecast Stock Price/ARIMAX_forecast_values.csv"
    dbms=csv;
run;


/*VAR Modeling 

proc varmax data=STOCK.PREPROCESSEDDATA(where=(date<'31JUL2020'd));
 id date interval=quarter;
 model close Vol_Close variancelast7 high_low VIX/
 minic=(type=aicc p=2 q=2);
 run;
 */
proc varmax data=STOCK.PREPROCESSEDDATA(where=(date<'31JUL2020'd)) plots=all;
 id date interval=day;
 model variancelast7  vol_close vix IntRate LastMonthPER close / p=3 lagmax=24;
 causal group1=(close) group2=(open);
 causal group1=(open) group2=(close);
 run;
 
 proc varmax data=STOCK.PREPROCESSEDDATA(where=(date<'31JUL2020'd));
 id date interval=day;
 model variancelast7  vol_close vix IntRate LastMonthPER close / p=3;
 output out=STOCK.VAR lead=90;
 run;

 proc export data=STOCK.VAR REPLACE 
    outfile="/home/u60762623/Forecast Stock Price/VAR_forecast_values.csv"
    dbms=csv;
run;

/*
ods trace on;
proc hpforest data=STOCK.PREPROCESSEDDATA_SPL maxtrees=100 vars_to_try=10 seed=1985
trainfraction=0.7 maxdepth=50 leafsize=6 alpha=0.5;
 target close /level=nominal;
 input  Vol_Close variancelast7 high_low VIX / level = interval;
ods output FitStatistics = fit_at_runtime;
ods output VariableImportance = Variable_Importance;
ods output Baseline = Baseline;
run;
ods trace off;
*/

/* Moving Average */


proc sort data=STOCK.PREPROCESSEDDATA_SPL out=STOCK.PREPROCESSEDDATA_SPL;
	by Date;
run;


proc arima data=STOCK.PREPROCESSEDDATA_SPL plots
     (only)=(series(corr crosscorr) residual(corr normal) 
		forecast(forecast) ) out=STOCK.MOVAVG;
	identify var=date;
	
	estimate q=(1 2 3 4 5 6 7) ma=(0.14285714285714285 0.14285714285714285 
		0.14285714285714285 0.14285714285714285 0.14285714285714285 
		0.14285714285714285 0.14285714285714285) noint method=CLS;
	forecast lead=90 back=0 alpha=0.05 id=date interval=day;
	run;
	
 proc export data=STOCK.MOVAVG REPLACE 
    outfile="/home/u60762623/Forecast Stock Price/MOVAVG_forecast_values.csv"
    dbms=csv;
run;


/* Exponential Smoothing */


proc esm data=STOCK.PREPROCESSEDDATA_SPL back=0 lead=90 plot=(corr errors 
		modelforecasts) out=STOCK.EXPSMOOTH;
	id Date interval=day;
	forecast Close / alpha=0.05 model=linear transform=none;
run;

	
 proc export data=STOCK.EXPSMOOTH REPLACE 
    outfile="/home/u60762623/Forecast Stock Price/EXPSMOOTH_forecast_values.csv"
    dbms=csv;
run;


/* ARIMA */

proc arima data=STOCK.PREPROCESSEDDATA_SPL plots
     (only)=(series(acf corr crosscorr pacf) residual(corr normal) 
		forecast(forecast forecastonly) ) out=STOCK.ARIMA;
	identify var=Close(1);
	estimate q=(1) method=ML;
	forecast lead=90 back=0 alpha=0.05 id=Date interval=day;
	run;
quit;


proc export data=STOCK.ARIMA REPLACE 
    outfile="/home/u60762623/Forecast Stock Price/ARIMA_forecast_values.csv"
    dbms=csv;
run;


/* RANDOM WALK */

proc arima data=STOCK.PREPROCESSEDDATA_SPL plots
     (only)=(series(acf corr crosscorr pacf) residual(corr normal) 
		forecast(forecast forecastonly) ) out=STOCK.RANDOM;
	identify var=Close (1);
	estimate method=CLS;
	forecast lead=90 back=0 alpha=0.05 id=Date interval=day;
	run;
quit;

proc export data=STOCK.RANDOM REPLACE 
    outfile="/home/u60762623/Forecast Stock Price/RANDOM_forecast_values.csv"
    dbms=csv;
run;


