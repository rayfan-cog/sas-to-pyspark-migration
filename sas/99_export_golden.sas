/*******************************************************************************/
/*  SAS Program: 99_export_golden.sas                                        */
/*  Purpose: Export golden outputs used to prove PySpark parity              */
/*  Run after 01-05 in the same SAS session, so the work library still       */
/*  holds home_equity, home_equity_final, and home_equity_risk.              */
/*                                                                           */
/*  Copy the four CSVs it writes into tests/parity/golden/ and see           */
/*  tests/parity/golden/README.md for the checklist.                         */
/*******************************************************************************/

%let outdir = /data/golden;

/* Row counts at each stage of the pipeline */
proc sql;
    create table work.row_counts as
        select 'home_equity'       as STAGE length=20, count(*) as N from work.home_equity
        union all
        select 'home_equity_final' as STAGE length=20, count(*) as N from work.home_equity_final
        union all
        select 'home_equity_risk'  as STAGE length=20, count(*) as N from work.home_equity_risk;
quit;

/* Frequency of the derived loan outcome */
proc freq data=work.home_equity_final noprint;
    tables LOAN_OUTCOME / out=work.freq_loan_outcome (rename=(COUNT=FREQUENCY));
run;

/* Group statistics that the PySpark summaryByGroup helper must reproduce */
proc means data=work.home_equity_final noprint;
    class LOAN_OUTCOME;
    var LOAN DEBTINC;
    output out=work.means_by_outcome (where=(_TYPE_=1))
        n=LOAN_COUNT DEBTINC_COUNT
        mean=LOAN_MEAN DEBTINC_MEAN
        std=LOAN_STDDEV DEBTINC_STDDEV;
run;

/* Distribution across the composite risk segments */
proc freq data=work.home_equity_risk noprint;
    tables RISK_SEGMENT / out=work.risk_segment_freq (rename=(COUNT=FREQUENCY));
run;

%macro dump(ds);
    proc export data=work.&ds outfile="&outdir./&ds..csv" dbms=csv replace;
    run;
%mend;

%dump(row_counts);
%dump(freq_loan_outcome);
%dump(means_by_outcome);
%dump(risk_segment_freq);
