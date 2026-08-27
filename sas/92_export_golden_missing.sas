/*******************************************************************************/
/*  SAS Program: 92_export_golden_missing.sas                                 */
/*  Purpose: Export the missingness golden set.                               */
/*  Run after 93_load_mock_missing.sas, 02_data_cleaning.sas and              */
/*  04_risk_segmentation.sas in the same session.                             */
/*                                                                            */
/*  Row-level on purpose: the fixture enumerates every missing/non-missing    */
/*  combination of the numeric predictors, so committing every derived value  */
/*  and every *_MISS flag per row gives the PySpark side an exact per-        */
/*  combination expectation of missing-flag and imputation semantics.         */
/*******************************************************************************/

%let outdir = /data/golden/missing;

/* Every derived value before any row is filtered out, with the source        */
/* predictor columns so a null-handling divergence shows the inputs too.      */
data work.miss_clean_rows;
    set work.home_equity_clean;
    keep CITY BAD LOAN MORTDUE VALUE REASON JOB YOJ DEROG DELINQ
         CLAGE NINQ CLNO DEBTINC LTV LOAN_OUTCOME;
    format _numeric_;
run;

/* Which rows survive 02's filters, with every *_MISS flag 02 sets and the    */
/* post-cleaning (imputed) values of the flagged columns.                     */
data work.miss_final_rows;
    set work.home_equity_final;
    keep CITY LOAN MORTDUE VALUE REASON JOB YOJ DEROG DELINQ
         CLAGE NINQ CLNO DEBTINC LTV LOAN_OUTCOME
         LOAN_MISS MORTDUE_MISS VALUE_MISS YOJ_MISS
         DEROG_MISS DELINQ_MISS CLAGE_MISS NINQ_MISS;
    format _numeric_;
run;

/* Category assignment and composite score for every surviving row */
data work.miss_risk_rows;
    set work.home_equity_risk;
    keep CITY LTV DEBTINC DELINQ DEROG
         LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT RISK_SCORE RISK_SEGMENT;
    format _numeric_;
run;

proc sql;
    create table work.miss_row_counts as
        select 'home_equity'          as STAGE length=22, count(*) as N from work.home_equity
        union all
        select 'home_equity_clean'    as STAGE length=22, count(*) as N from work.home_equity_clean
        union all
        select 'home_equity_imputed'  as STAGE length=22, count(*) as N from work.home_equity_imputed
        union all
        select 'home_equity_filtered' as STAGE length=22, count(*) as N from work.home_equity_filtered
        union all
        select 'home_equity_final'    as STAGE length=22, count(*) as N from work.home_equity_final
        union all
        select 'home_equity_risk'     as STAGE length=22, count(*) as N from work.home_equity_risk;
quit;

/* Sums of every *_MISS flag over the surviving rows */
proc sql;
    create table work.miss_flag_totals as
        select sum(LOAN_MISS)    as LOAN_MISS_TOTAL,
               sum(MORTDUE_MISS) as MORTDUE_MISS_TOTAL,
               sum(VALUE_MISS)   as VALUE_MISS_TOTAL,
               sum(YOJ_MISS)     as YOJ_MISS_TOTAL,
               sum(DEROG_MISS)   as DEROG_MISS_TOTAL,
               sum(DELINQ_MISS)  as DELINQ_MISS_TOTAL,
               sum(CLAGE_MISS)   as CLAGE_MISS_TOTAL,
               sum(NINQ_MISS)    as NINQ_MISS_TOTAL
        from work.home_equity_final;
quit;

proc freq data=work.home_equity_risk noprint;
    tables RISK_SEGMENT / out=work.miss_risk_segment_freq (rename=(COUNT=FREQUENCY));
run;

%macro dump_miss(ds);
    proc export data=work.&ds outfile="&outdir./&ds..csv" dbms=csv replace;
    run;
%mend;

%dump_miss(miss_row_counts);
%dump_miss(miss_clean_rows);
%dump_miss(miss_final_rows);
%dump_miss(miss_risk_rows);
%dump_miss(miss_flag_totals);
%dump_miss(miss_risk_segment_freq);
