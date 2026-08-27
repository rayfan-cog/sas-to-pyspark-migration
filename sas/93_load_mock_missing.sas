/*******************************************************************************/
/*  SAS Program: 93_load_mock_missing.sas                                     */
/*  Purpose: Load the generated missingness fixture into work.home_equity so  */
/*           02_data_cleaning.sas and 04_risk_segmentation.sas can be re-run  */
/*           over it, producing a missingness-focused golden set.             */
/*                                                                            */
/*  data/home_equity_missing.csv (built by tools/generate_mock_missing.py)    */
/*  enumerates all 512 missing/non-missing combinations of the 9 numeric      */
/*  predictors, plus JOB/REASON-missing rows and a median/mode anchor block.  */
/*  CITY carries a label naming the combination each row covers, so the       */
/*  row-level goldens name the rule that diverged.                            */
/*                                                                            */
/*  Run this AFTER the production and edge passes have exported their CSVs:   */
/*  it overwrites work.home_equity, and 02/04 then overwrite the downstream   */
/*  datasets.                                                                 */
/*******************************************************************************/

%let missfile = /data/home_equity_missing.csv;

proc import datafile="&missfile."
    dbms=csv
    out=work.home_equity
    replace;
    guessingrows=max;
run;

title "Missingness fixture as imported";
proc print data=work.home_equity(obs=20);
    var BAD LOAN MORTDUE VALUE YOJ DEROG DELINQ CLAGE NINQ CLNO DEBTINC CITY;
run;
title;
