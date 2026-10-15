CREATE SCHEMA IF NOT EXISTS risk;

CREATE TABLE risk.risk_analysis (

    loan_identifier VARCHAR(32) NOT NULL,

    classic_fico INTEGER,
    vantagescore_4_0 INTEGER,

    original_dti DOUBLE PRECISION,
    original_ltv DOUBLE PRECISION,
    original_cltv DOUBLE PRECISION,

    original_upb DOUBLE PRECISION,
    original_interest_rate DOUBLE PRECISION,
    mortgage_insurance_percentage DOUBLE PRECISION,

    number_of_borrowers INTEGER,
    number_of_units INTEGER,

    first_time_homebuyer_indicator VARCHAR(8),
    occupancy_status VARCHAR(8),
    loan_purpose VARCHAR(8),
    channel VARCHAR(8),

    property_type VARCHAR(8),
    property_state VARCHAR(8),

    original_loan_term INTEGER,

    total_observed_months BIGINT,
    observed_months_12m BIGINT,
    observed_months_24m BIGINT,

    first_observed_period VARCHAR(6),

    max_delinquency_12m INTEGER,
    max_delinquency_24m INTEGER,

    ever_30dpd_12m INTEGER,
    ever_60dpd_12m INTEGER,
    ever_90dpd_12m INTEGER,

    ever_30dpd_24m INTEGER,
    ever_60dpd_24m INTEGER,
    ever_90dpd_24m INTEGER,

    vintage_year INTEGER,
    vintage_quarter VARCHAR(2)
);


CREATE TABLE risk.portfolio_monthly (
    period VARCHAR(6) NOT NULL,

    observed_loan_count BIGINT,
    active_loan_count BIGINT,
    active_upb DOUBLE PRECISION,

    current_loan_count BIGINT,
    current_upb DOUBLE PRECISION,

    dpd30_loan_count BIGINT,
    dpd30_upb DOUBLE PRECISION,

    dpd60_loan_count BIGINT,
    dpd60_upb DOUBLE PRECISION,

    dpd90plus_loan_count BIGINT,
    dpd90plus_upb DOUBLE PRECISION,

    unknown_delinquency_count BIGINT,
    prepay_count BIGINT,

    vintage_year INTEGER NOT NULL,
    vintage_quarter VARCHAR(2) NOT NULL,

    PRIMARY KEY (
        vintage_year,
        vintage_quarter,
        period
    )
);

CREATE TABLE risk.delinquency_transition (
    period VARCHAR(6) NOT NULL,

    from_state VARCHAR(20) NOT NULL,
    to_state VARCHAR(20) NOT NULL,

    transition_loan_count BIGINT,

    from_upb DOUBLE PRECISION,
    to_upb DOUBLE PRECISION,

    vintage_year INTEGER NOT NULL,
    vintage_quarter VARCHAR(2) NOT NULL,

    PRIMARY KEY (
        vintage_year,
        vintage_quarter,
        period,
        from_state,
        to_state
    )
);

CREATE TABLE risk.loan_lifecycle (
    loan_identifier VARCHAR(32) NOT NULL,

    first_observed_period VARCHAR(6),
    last_observed_period VARCHAR(6),

    total_observed_months BIGINT,

    first_30dpd_period VARCHAR(6),
    first_60dpd_period VARCHAR(6),
    first_90dpd_period VARCHAR(6),

    max_delinquency INTEGER,

    ever_30dpd INTEGER,
    ever_60dpd INTEGER,
    ever_90dpd INTEGER,

    terminal_period VARCHAR(6),
    zero_balance_code VARCHAR(2),

    last_observed_upb DOUBLE PRECISION,

    vintage_year INTEGER NOT NULL,
    vintage_quarter VARCHAR(2) NOT NULL,

    PRIMARY KEY (
        loan_identifier
    )
);