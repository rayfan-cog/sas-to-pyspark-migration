/*******************************************************************************/
/*  SAS Program: 98_export_golden_edge.sas                                   */
/*  Purpose: Export the edge-case golden set.                                 */
/*  Run after 97_load_edge_cases.sas, 02_data_cleaning.sas and                */
/*  04_risk_segmentation.sas in the same session.                             */
/*                                                                            */
/*  These exports are row-level on purpose: the fixture is 38 synthetic rows  */
/*  built for this repo, so committing every derived value gives the PySpark  */
/*  side an exact, per-case expectation instead of an aggregate that can hide */
/*  two compensating errors.                                                  */
/*******************************************************************************/

%let outdir = /data/golden;

/* Every derived value before any row is filtered out: this is where a        */
/* PySpark null-vs-missing divergence shows up first.                         */
data work.edge_clean_rows;
    set work.home_equity_clean;
    keep CITY BAD LOAN MORTDUE VALUE DEBTINC DELINQ DEROG LTV LOAN_OUTCOME;
    format _numeric_;
run;

/* Which rows survive 02's filters, with the missing flags 02 sets */
data work.edge_final_rows;
    set work.home_equity_final;
    keep CITY LTV LOAN_OUTCOME
         LOAN_MISS MORTDUE_MISS VALUE_MISS YOJ_MISS
         DEROG_MISS DELINQ_MISS CLAGE_MISS NINQ_MISS;
    format _numeric_;
run;

/* Category assignment and composite score for every surviving row */
data work.edge_risk_rows;
    set work.home_equity_risk;
    keep CITY LTV DEBTINC DELINQ DEROG
         LTV_RISK_CAT DTI_RISK_CAT DELINQ_RISK_CAT RISK_SCORE RISK_SEGMENT;
    format _numeric_;
run;

proc sql;
    create table work.edge_row_counts as
        select 'home_equity'       as STAGE length=20, count(*) as N from work.home_equity
        union all
        select 'home_equity_clean' as STAGE length=20, count(*) as N from work.home_equity_clean
        union all
        select 'home_equity_final' as STAGE length=20, count(*) as N from work.home_equity_final
        union all
        select 'home_equity_risk'  as STAGE length=20, count(*) as N from work.home_equity_risk;
quit;

proc freq data=work.home_equity_risk noprint;
    tables RISK_SEGMENT / out=work.edge_risk_segment_freq (rename=(COUNT=FREQUENCY));
run;

%macro dump_edge(ds);
    proc export data=work.&ds outfile="&outdir./&ds..csv" dbms=csv replace;
    run;
%mend;

%dump_edge(edge_row_counts);
%dump_edge(edge_clean_rows);
%dump_edge(edge_final_rows);
%dump_edge(edge_risk_rows);
%dump_edge(edge_risk_segment_freq);
