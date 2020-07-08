'''
Define create table scripts to populate "truth" tables from staging tables.

Core extraction and data transformation performed during table load.
'''


class SqlQueries:
    build_states = ("""
CREATE TABLE IF NOT EXISTS states AS
SELECT
    row_number() OVER (
        ORDER BY
            v1.state
    ) id_pk,
    cast(v1.state AS varchar)
FROM
    (
        SELECT
            DISTINCT upper(state) AS state
        FROM
            stage_us_city_demographics
        UNION
        SELECT
            DISTINCT upper(state) AS state
        FROM
            stage_min_wage
    ) v1
    """)

    build_case_status = ("""
CREATE TABLE IF NOT EXISTS case_status AS
SELECT
    row_number() OVER (
        ORDER BY
            v1.case_status
    ) id_pk,
    cast(v1.case_status AS varchar)
FROM
    (
        SELECT
            DISTINCT upper(case_status) AS case_status
        FROM
            stage_h1b_petitions
    ) v1
    """)

    build_min_wage = ("""
CREATE TABLE IF NOT EXISTS min_wage AS
SELECT
    row_number() over(
        ORDER BY
            v1.year,
            fk_states
    ) id_pk,
    v1.year,
    v1.hi_value,
    v1.lo_value,
    fk_states
FROM
    (
        SELECT
            DISTINCT smw.year,
            smw.hi_value,
            smw.lo_value,
            s.id_pk AS fk_states
        FROM
            stage_min_wage smw,
            states s
        WHERE
            upper(smw.state) = s.state
    ) v1
    """)

    build_truth_us_city_demographics = ("""
CREATE TABLE IF NOT EXISTS us_city_demographics AS
SELECT
    row_number() over(
        ORDER BY
            fk_states,
            city
    ) AS id_pk,
    v1.city,
    v1.median_age,
    v1.male_population,
    v1.female_population,
    v1.number_of_veterans,
    v1.foreign_born,
    v1.average_household_size,
    v1.race,
    v1.cnt,
    v1.fk_states
FROM
    (
        SELECT
            DISTINCT upper(city) AS city,
            median_age,
            male_population,
            female_population,
            number_of_veterans,
            foreign_born,
            average_household_size,
            race,
            cnt,
            s.id_pk AS fk_states
        FROM
            stage_us_city_demographics sucd,
            states s
        WHERE
            upper(sucd.state) = s.state
    ) v1
    """)

    build_truth_world_happiness = ("""
CREATE TABLE IF NOT EXISTS world_happiness AS
SELECT
    row_number() over(
        ORDER BY
            v1.year,
            v1.country DESC
    ) AS id_pk,
    upper(v1.country) AS country,
    v1.year,
    v1.happiness,
    RANK () OVER (
        PARTITION BY v1.year
        ORDER BY
            v1.year,
            v1.happiness DESC
    ) happiness_rank_for_yr,
    v1.log_gdp_per_cap,
    v1.social_support,
    v1.life_expectancy,
    v1.freedom,
    v1.generosity,
    v1.corruption_perception,
    v1.positive_affect,
    v1.negative_affect,
    v1.confidence_in_government,
    v1.democratic_quality,
    v1.delivery_quality,
    v1.happiness_sd,
    v1.happiness_sd_mean,
    v1.gini_index,
    v1.gini_index_2000_to_2015,
    v1.household_income_gini
FROM
    (
        SELECT
            country,
            year,
            life_ladder AS happiness,
            log_gdp_per_capita AS log_gdp_per_cap,
            social_support,
            healthy_life_expectancy_at_birth AS life_expectancy,
            freedom_to_make_life_choices AS freedom,
            generosity,
            perceptions_of_corruption AS corruption_perception,
            positive_affect,
            negative_affect,
            confidence_in_national_government AS confidence_in_government,
            democratic_quality,
            delivery_quality,
            standard_dev_of_ladder happiness_sd,
            standard_dev_mean_of_ladder happiness_sd_mean,
            gini_index gini_index,
            gini_index_est gini_index_2000_to_2015,
            gini_of_household_income household_income_gini
        FROM
            stage_world_happiness
    ) v1
    """)

    build_truth_h1b_nationality = ("""
CREATE TABLE IF NOT EXISTS h1b_nationality AS
SELECT
    row_number() over(
        ORDER BY
            v1.year
    ) id_pk,
    v1.year,
    v1.nationality,
    v1.cnt
FROM
    (
        SELECT
            DISTINCT year,
            nationality,
            count AS cnt
        FROM
            stage_nationality
        WHERE
            year IS NOT NULL
    ) v1
    """)

    build_truth_h1b_petitions = ("""
CREATE TABLE IF NOT EXISTS h1b_petitions AS
SELECT
    cast(shp.id AS numeric) AS id_pk,
    (
        SELECT
            id_pk
        FROM
            case_status
        WHERE
            case_status = shp.case_status
    ) fk_case_status,
    shp.employer_name,
    shp.soc_name,
    shp.job_title,
    shp.full_time_position,
    shp.prevailing_wage,
    CASE
        WHEN shp.year = 'NA' THEN NULL
        ELSE cast(shp.year AS numeric)
    END AS year,
    cast(
        upper(split_part(shp.worksite, ',', 1)) AS varchar
    ) AS city,
    (
        SELECT
            id_pk
        FROM
            states
        WHERE
            state = trim(upper(split_part(shp.worksite, ',', 2)))
    ) fk_states,
    CASE
        WHEN shp.lon = 'NA' THEN NULL
        ELSE cast(shp.lon AS numeric)
    END AS lon,
    CASE
        WHEN shp.lat = 'NA' THEN NULL
        ELSE cast(shp.lat AS numeric)
    END AS lat
FROM
    stage_h1b_petitions shp
    """)
