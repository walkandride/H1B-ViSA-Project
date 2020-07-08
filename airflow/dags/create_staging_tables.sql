-- =============================================================================
-- Create staging tables for import files.
-- =============================================================================

CREATE TABLE IF NOT EXISTS stage_h1b_petitions(
    id VARCHAR,
    case_status VARCHAR,
    employer_name VARCHAR,
    soc_name VARCHAR,
    job_title VARCHAR,
    full_time_position VARCHAR,
    prevailing_wage VARCHAR,
    year VARCHAR,
    worksite VARCHAR,
    lon VARCHAR,
    lat VARCHAR
);


CREATE TABLE IF NOT EXISTS stage_nationality(
    year INT,
    type VARCHAR,
    nationality VARCHAR,
    count INT
);


CREATE TABLE IF NOT EXISTS stage_min_wage(
    year INT,
    state VARCHAR,
    table_data VARCHAR,
    footnote VARCHAR,
    hi_value VARCHAR,
    lo_value VARCHAR,
    cpi_avg VARCHAR,
    hi_2018 VARCHAR,
    lo_2018 VARCHAR
);


CREATE TABLE IF NOT EXISTS stage_world_happiness(
    country VARCHAR,
    year INT,
    life_ladder NUMERIC,
    log_gdp_per_capita NUMERIC,
    social_support NUMERIC,
    healthy_life_expectancy_at_birth NUMERIC,
    freedom_to_make_life_choices NUMERIC,
    generosity NUMERIC,
    perceptions_of_corruption NUMERIC,
    positive_affect NUMERIC,
    negative_affect NUMERIC,
    confidence_in_national_government NUMERIC,
    democratic_quality NUMERIC,
    delivery_quality NUMERIC,
    standard_dev_of_ladder NUMERIC,
    standard_dev_mean_of_ladder NUMERIC,
    gini_index NUMERIC,
    gini_index_est NUMERIC,
    gini_of_household_income NUMERIC
);


CREATE TABLE IF NOT EXISTS stage_us_city_demographics(
    city VARCHAR,
    state VARCHAR,
    median_age NUMERIC,
    male_population INT,
    female_population INT,
    total_population INT,
    number_of_veterans INT,
    foreign_born INT,
    average_household_size NUMERIC,
    state_code VARCHAR,
    race VARCHAR,
    cnt INT
);

