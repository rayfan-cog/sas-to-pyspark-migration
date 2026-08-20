/*******************************************************************************/
/*  SAS Program: 97_load_edge_cases.sas                                      */
/*  Purpose: Load the hand-built edge-case fixture into work.home_equity so   */
/*           02_data_cleaning.sas and 04_risk_segmentation.sas can be re-run  */
/*           over it, producing a second, boundary-focused golden set.        */
/*                                                                            */
/*  The production extract exercises the happy path; it has no row sitting    */
/*  exactly on an LTV / DEBTINC / DELINQ / RISK_SCORE threshold and no row    */
/*  that is rejected for each distinct reason. data/home_equity_edge.csv      */
/*  supplies one row per boundary and per rejection rule, and CITY carries a  */
/*  short label naming the case the row covers.                               */
/*                                                                            */
/*  Run this AFTER the production pass has exported its CSVs: it overwrites   */
/*  work.home_equity, and 02/04 then overwrite the downstream datasets.       */
/*******************************************************************************/

%let edgefile = /data/home_equity_edge.csv;

proc import datafile="&edgefile."
    dbms=csv
    out=work.home_equity
    replace;
    guessingrows=max;
run;

title "Edge-case fixture as imported";
proc print data=work.home_equity;
    var BAD LOAN MORTDUE VALUE DEBTINC DELINQ DEROG CITY;
run;
title;
