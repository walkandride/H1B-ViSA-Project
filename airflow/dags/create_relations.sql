-- =============================================================================
-- Create table primary keys, relationships, and indexes.
-- =============================================================================

-- define primary keys
ALTER TABLE
    states
ADD
    CONSTRAINT states_pk PRIMARY KEY (id_pk);

ALTER TABLE
    case_status
ADD
    CONSTRAINT case_status_pk PRIMARY KEY (id_pk);

ALTER TABLE
    min_wage
ADD
    CONSTRAINT min_wage_pk PRIMARY KEY (id_pk);

ALTER TABLE
    h1b_nationality
ADD
    CONSTRAINT h1b_nationality_pk PRIMARY KEY (id_pk);

ALTER TABLE
    world_happiness
ADD
    CONSTRAINT world_happiness_pk PRIMARY KEY (id_pk);

ALTER TABLE
    us_city_demographics
ADD
    CONSTRAINT us_city_demographics_pk PRIMARY KEY (id_pk);

ALTER TABLE
    h1b_petitions
ADD
    CONSTRAINT h1b_petitions_pk PRIMARY KEY (id_pk);

-- define foreign keys    
ALTER TABLE
    min_wage
ADD
    CONSTRAINT mw_fk_states_fk FOREIGN KEY(fk_states) REFERENCES states(id_pk);

ALTER TABLE
    us_city_demographics
ADD
    CONSTRAINT ucd_fk_states_fk FOREIGN KEY(fk_states) REFERENCES states(id_pk);

ALTER TABLE
    h1b_petitions
ADD
    CONSTRAINT h1bp_fk_states_fk FOREIGN KEY(fk_states) REFERENCES states(id_pk);

ALTER TABLE
    h1b_petitions
ADD
    CONSTRAINT h1bp_fk_case_status_fk FOREIGN KEY(fk_case_status) REFERENCES case_status(id_pk);

-- define unique constraints
ALTER TABLE
    states
ADD
    CONSTRAINT s_state_uq UNIQUE (state);

ALTER TABLE
    case_status
ADD
    CONSTRAINT case_status_uq1 UNIQUE (case_status);

ALTER TABLE
    min_wage
ADD
    CONSTRAINT min_wage_uq1 UNIQUE (year, fk_states);

-- define indexes
CREATE INDEX h1b_petitions_idx1 ON h1b_petitions (year, fk_case_status);

CREATE INDEX h1b_petitions_idx2 ON h1b_petitions (city, fk_states);

CREATE INDEX h1b_nationality_idx1 ON h1b_nationality (year, nationality);

CREATE INDEX us_city_demographics_idx1 ON us_city_demographics (city, fk_states);