/*******************************************************************************/
/*  SAS Program: 99_export_golden.sas                                        */
/*  Purpose: Export golden outputs used to prove PySpark parity              */
/*  Run after 01-05 in the same SAS session, so the work library still       */
/*  holds home_equity, home_equity_clean, home_equity_final and              */
/*  home_equity_risk.                                                        */
/*                                                                           */
/*  Copy the CSVs it writes into tests/parity/golden/ and see                */
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

/* JOB and REASON both carry missing values, so these tables pin the         */
/* missing-class behaviour: PROC FREQ drops missings from the percent base   */
/* by default but still emits a blank-category row.                          */
proc freq data=work.home_equity_final noprint;
    tables JOB    / out=work.freq_job    (rename=(COUNT=FREQUENCY));
    tables REASON / out=work.freq_reason (rename=(COUNT=FREQUENCY));
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

/* Distributions across the risk categories and the composite segment */
proc freq data=work.home_equity_risk noprint;
    tables RISK_SEGMENT    / out=work.risk_segment_freq   (rename=(COUNT=FREQUENCY));
    tables LTV_RISK_CAT    / out=work.freq_ltv_risk_cat   (rename=(COUNT=FREQUENCY));
    tables DTI_RISK_CAT    / out=work.freq_dti_risk_cat   (rename=(COUNT=FREQUENCY));
    tables DELINQ_RISK_CAT / out=work.freq_delinq_risk_cat(rename=(COUNT=FREQUENCY));
    tables RISK_SCORE      / out=work.freq_risk_score     (rename=(COUNT=FREQUENCY));
run;

/* Two-way segment x outcome counts (the cross-tab 04 prints) */
proc freq data=work.home_equity_risk noprint;
    tables RISK_SEGMENT * LOAN_OUTCOME / out=work.crosstab_risk_outcome (rename=(COUNT=FREQUENCY));
run;

/* Per-segment statistics, including the default rate */
proc means data=work.home_equity_risk noprint;
    class RISK_SEGMENT;
    var BAD LOAN LTV DEBTINC;
    output out=work.means_by_risk_segment (where=(_TYPE_=1))
        n=BAD_COUNT LOAN_COUNT LTV_COUNT DEBTINC_COUNT
        mean=BAD_MEAN LOAN_MEAN LTV_MEAN DEBTINC_MEAN
        std=BAD_STDDEV LOAN_STDDEV LTV_STDDEV DEBTINC_STDDEV;
run;

/* Distribution shape of every continuous column. Percentiles are the        */
/* sharpest parity test available: SAS PCTLDEF=5 and Spark's                 */
/* approxQuantile/percentile differ unless the PySpark side asks for an      */
/* exact percentile with matching interpolation.                             */
proc means data=work.home_equity_final noprint;
    var LOAN MORTDUE VALUE DEBTINC LTV CLAGE YOJ CLNO;
    output out=work.quantiles_numeric
        n=      LOAN_N       MORTDUE_N       VALUE_N       DEBTINC_N       LTV_N       CLAGE_N       YOJ_N       CLNO_N
        nmiss=  LOAN_NMISS   MORTDUE_NMISS   VALUE_NMISS   DEBTINC_NMISS   LTV_NMISS   CLAGE_NMISS   YOJ_NMISS   CLNO_NMISS
        min=    LOAN_MIN     MORTDUE_MIN     VALUE_MIN     DEBTINC_MIN     LTV_MIN     CLAGE_MIN     YOJ_MIN     CLNO_MIN
        p25=    LOAN_P25     MORTDUE_P25     VALUE_P25     DEBTINC_P25     LTV_P25     CLAGE_P25     YOJ_P25     CLNO_P25
        median= LOAN_MEDIAN  MORTDUE_MEDIAN  VALUE_MEDIAN  DEBTINC_MEDIAN  LTV_MEDIAN  CLAGE_MEDIAN  YOJ_MEDIAN  CLNO_MEDIAN
        p75=    LOAN_P75     MORTDUE_P75     VALUE_P75     DEBTINC_P75     LTV_P75     CLAGE_P75     YOJ_P75     CLNO_P75
        max=    LOAN_MAX     MORTDUE_MAX     VALUE_MAX     DEBTINC_MAX     LTV_MAX     CLAGE_MAX     YOJ_MAX     CLNO_MAX;
run;

/* Totals of the eight missing-value flags 02 builds with array processing */
proc means data=work.home_equity_final noprint;
    var LOAN_MISS MORTDUE_MISS VALUE_MISS YOJ_MISS DEROG_MISS DELINQ_MISS CLAGE_MISS NINQ_MISS;
    output out=work.missing_flag_totals
        sum=LOAN_MISS MORTDUE_MISS VALUE_MISS YOJ_MISS DEROG_MISS DELINQ_MISS CLAGE_MISS NINQ_MISS;
run;

/* PROC MEANS output inherits the DOLLAR/percent formats of its source       */
/* columns, which rounds the exported values. Keep that formatted view for   */
/* fidelity to what the procedure prints, but export the format-stripped     */
/* dataset as the file the parity tests compare against.                     */
%macro keep_formatted(ds);
    data work.&ds._formatted;
        set work.&ds;
    run;
    proc datasets lib=work nolist;
        modify &ds;
            format _numeric_;
    quit;
%mend;

%keep_formatted(means_by_outcome);
%keep_formatted(means_by_risk_segment);

proc datasets lib=work nolist;
    modify quantiles_numeric;
        format _numeric_;
    modify missing_flag_totals;
        format _numeric_;
    modify freq_risk_score;
        format _numeric_;
quit;

%macro dump(ds);
    proc export data=work.&ds outfile="&outdir./&ds..csv" dbms=csv replace;
    run;
%mend;

%dump(row_counts);
%dump(freq_loan_outcome);
%dump(freq_job);
%dump(freq_reason);
%dump(means_by_outcome);
%dump(means_by_outcome_formatted);
%dump(risk_segment_freq);
%dump(freq_ltv_risk_cat);
%dump(freq_dti_risk_cat);
%dump(freq_delinq_risk_cat);
%dump(freq_risk_score);
%dump(crosstab_risk_outcome);
%dump(means_by_risk_segment);
%dump(means_by_risk_segment_formatted);
%dump(quantiles_numeric);
%dump(missing_flag_totals);
