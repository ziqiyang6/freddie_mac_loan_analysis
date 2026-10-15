CREATE OR REPLACE VIEW risk.transition_rates AS

WITH from_totals AS (

    SELECT
        period,
        vintage_year,
        vintage_quarter,
        from_state,

        SUM(transition_loan_count) AS from_loan_count,
        SUM(from_upb) AS from_total_upb

    FROM risk.delinquency_transition

    GROUP BY
        period,
        vintage_year,
        vintage_quarter,
        from_state
),

to_states(to_state) AS (

    VALUES
        ('Current'::VARCHAR(20)),
        ('30 DPD'::VARCHAR(20)),
        ('60 DPD'::VARCHAR(20)),
        ('90+ DPD'::VARCHAR(20)),
        ('Exit'::VARCHAR(20)),
        ('Unknown'::VARCHAR(20))
)

SELECT
    f.period,
    f.vintage_year,
    f.vintage_quarter,

    f.from_state,
    s.to_state,

    COALESCE(
        d.transition_loan_count,
        0
    ) AS transition_loan_count,

    COALESCE(
        d.from_upb,
        0.0
    ) AS from_upb,

    f.from_loan_count,
    f.from_total_upb

FROM from_totals f

CROSS JOIN to_states s

LEFT JOIN risk.delinquency_transition d
    ON  d.period = f.period
    AND d.vintage_year = f.vintage_year
    AND d.vintage_quarter = f.vintage_quarter
    AND d.from_state = f.from_state
    AND d.to_state = s.to_state;