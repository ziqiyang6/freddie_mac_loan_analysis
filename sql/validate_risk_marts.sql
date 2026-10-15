-- ============================================================
-- Freddie Mac Risk Mart Validation
-- ============================================================


-- ============================================================
-- 0. Refresh PostgreSQL statistics
-- ============================================================

ANALYZE risk.risk_analysis;
ANALYZE risk.portfolio_monthly;
ANALYZE risk.delinquency_transition;
ANALYZE risk.loan_lifecycle;



-- ============================================================
-- 1. Basic row counts
-- ============================================================

SELECT
    'risk_analysis' AS table_name,
    COUNT(*) AS row_count
FROM risk.risk_analysis

UNION ALL

SELECT
    'portfolio_monthly',
    COUNT(*)
FROM risk.portfolio_monthly

UNION ALL

SELECT
    'delinquency_transition',
    COUNT(*)
FROM risk.delinquency_transition

UNION ALL

SELECT
    'loan_lifecycle',
    COUNT(*)
FROM risk.loan_lifecycle;



-- ============================================================
-- 2. Loan-level tables should contain same number of loans
-- ============================================================

SELECT
    (SELECT COUNT(*)
     FROM risk.risk_analysis)
        AS risk_analysis_loans,

    (SELECT COUNT(*)
     FROM risk.loan_lifecycle)
        AS lifecycle_loans,

    CASE
        WHEN
            (SELECT COUNT(*)
             FROM risk.risk_analysis)
            =
            (SELECT COUNT(*)
             FROM risk.loan_lifecycle)
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status;



-- ============================================================
-- 3. Check loan coverage between risk_analysis and lifecycle
-- ============================================================

SELECT
    COUNT(*) AS loans_missing_from_lifecycle
FROM risk.risk_analysis r
LEFT JOIN risk.loan_lifecycle l
    ON r.loan_identifier = l.loan_identifier
WHERE l.loan_identifier IS NULL;


SELECT
    COUNT(*) AS loans_missing_from_risk_analysis
FROM risk.loan_lifecycle l
LEFT JOIN risk.risk_analysis r
    ON l.loan_identifier = r.loan_identifier
WHERE r.loan_identifier IS NULL;



-- ============================================================
-- 4. Portfolio monthly reconciliation
--
-- Active loans should equal:
--
-- Current
-- + 30 DPD
-- + 60 DPD
-- + 90+ DPD
-- + Unknown
-- ============================================================

SELECT
    COUNT(*) AS invalid_months
FROM risk.portfolio_monthly
WHERE active_loan_count
      <>
      (
          current_loan_count
          + dpd30_loan_count
          + dpd60_loan_count
          + dpd90plus_loan_count
          + unknown_delinquency_count
      );



-- ============================================================
-- 5. Active loans cannot exceed observed loans
-- ============================================================

SELECT
    COUNT(*) AS invalid_months
FROM risk.portfolio_monthly
WHERE active_loan_count > observed_loan_count;



-- ============================================================
-- 6. Prepay cannot exceed all exited loans
--
-- All exits:
-- observed - active
--
-- Prepay is only one type of exit.
-- ============================================================

SELECT
    COUNT(*) AS invalid_months
FROM risk.portfolio_monthly
WHERE prepay_count
      >
      observed_loan_count - active_loan_count;



-- ============================================================
-- 7. Portfolio loan-months should reconcile to lifecycle
--
-- Portfolio:
-- SUM(observed_loan_count)
--
-- Lifecycle:
-- SUM(total_observed_months)
--
-- Both represent total Perf loan-month observations.
-- ============================================================

SELECT
    portfolio.total_observed_rows,
    lifecycle.total_observed_rows,

    portfolio.total_observed_rows
        - lifecycle.total_observed_rows
        AS difference,

    CASE
        WHEN portfolio.total_observed_rows
             = lifecycle.total_observed_rows
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status

FROM
(
    SELECT
        SUM(observed_loan_count)
            AS total_observed_rows
    FROM risk.portfolio_monthly
) portfolio

CROSS JOIN

(
    SELECT
        SUM(total_observed_months)
            AS total_observed_rows
    FROM risk.loan_lifecycle
) lifecycle;



-- ============================================================
-- 8. Lifecycle DPD hierarchy
--
-- ever 90+ implies ever 60+
-- ever 60+ implies ever 30+
-- ============================================================

SELECT
    COUNT(*) AS invalid_loans
FROM risk.loan_lifecycle
WHERE ever_30dpd < ever_60dpd
   OR ever_60dpd < ever_90dpd;



-- ============================================================
-- 9. Lifecycle observation period validity
-- ============================================================

SELECT
    COUNT(*) AS invalid_loans
FROM risk.loan_lifecycle
WHERE first_observed_period
      > last_observed_period;



-- ============================================================
-- 10. First delinquency period hierarchy
-- ============================================================

SELECT
    COUNT(*) AS invalid_loans
FROM risk.loan_lifecycle

WHERE
    (
        first_60dpd_period IS NOT NULL
        AND
        (
            first_30dpd_period IS NULL
            OR first_30dpd_period > first_60dpd_period
        )
    )

    OR

    (
        first_90dpd_period IS NOT NULL
        AND
        (
            first_60dpd_period IS NULL
            OR first_60dpd_period > first_90dpd_period
        )
    );



-- ============================================================
-- 11. Terminal information consistency
-- ============================================================

SELECT
    COUNT(*) AS invalid_loans
FROM risk.loan_lifecycle

WHERE
    (
        zero_balance_code IS NULL
        AND terminal_period IS NOT NULL
    )

    OR

    (
        zero_balance_code IS NOT NULL
        AND
        (
            terminal_period IS NULL
            OR terminal_period <> last_observed_period
        )
    );



-- ============================================================
-- 12. Transition states
-- ============================================================

SELECT
    from_state,
    to_state,
    COUNT(*) AS rows
FROM risk.delinquency_transition
WHERE from_state NOT IN (
          'Current',
          '30 DPD',
          '60 DPD',
          '90+ DPD',
          'Exit',
          'Unknown'
      )
   OR to_state NOT IN (
          'Current',
          '30 DPD',
          '60 DPD',
          '90+ DPD',
          'Exit',
          'Unknown'
      )
GROUP BY
    from_state,
    to_state;



-- ============================================================
-- 13. Transition count sanity check
--
-- Maximum possible transitions:
--
-- total Perf rows
-- - number of loans
--
-- Actual may be slightly smaller because we exclude
-- non-consecutive calendar-month observations.
-- ============================================================

WITH lifecycle AS (

    SELECT
        SUM(total_observed_months)
            AS total_perf_rows,

        COUNT(*)
            AS total_loans

    FROM risk.loan_lifecycle

),

transition AS (

    SELECT
        SUM(transition_loan_count)
            AS actual_transitions

    FROM risk.delinquency_transition

)

SELECT
    lifecycle.total_perf_rows,
    lifecycle.total_loans,

    lifecycle.total_perf_rows
        - lifecycle.total_loans
        AS maximum_possible_transitions,

    transition.actual_transitions,

    (
        lifecycle.total_perf_rows
        - lifecycle.total_loans
        - transition.actual_transitions
    ) AS excluded_transition_gaps,

    CASE
        WHEN transition.actual_transitions
             <=
             lifecycle.total_perf_rows
             - lifecycle.total_loans
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status

FROM lifecycle
CROSS JOIN transition;



-- ============================================================
-- 14. Check expected loan count
--
-- Current project total:
-- 13,589,191 loans
-- ============================================================

SELECT
    COUNT(*) AS loan_count,

    CASE
        WHEN COUNT(*) = 13589191
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status

FROM risk.risk_analysis;



-- ============================================================
-- 15. Final quick portfolio summary
-- ============================================================

SELECT
    MIN(period) AS first_period,
    MAX(period) AS last_period,

    COUNT(
        DISTINCT
        (
            vintage_year,
            vintage_quarter
        )
    ) AS vintage_count,

    SUM(observed_loan_count)
        AS total_loan_months,

    SUM(dpd90plus_loan_count)
        AS total_90plus_observations,

    ROUND(
        SUM(dpd90plus_loan_count)::NUMERIC
        /
        NULLIF(
            SUM(active_loan_count),
            0
        ),
        6
    ) AS overall_90plus_share

FROM risk.portfolio_monthly;