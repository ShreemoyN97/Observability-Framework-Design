Use DAMG_7374_ADG_Processed;
CREATE TABLE Processed_Lobbying_Data (
    record_id INT AUTO_INCREMENT PRIMARY KEY,                -- Unique identifier for each processed record
    form_submission_id INT,                                  -- Identifier for form submission
    reporting_year INT,                                      -- Year of reporting
    filing_type VARCHAR(50),                                 -- Type of filing (e.g., Amendment)
    reporting_period VARCHAR(50),                            -- Reporting period (e.g., Jan/Feb)
    principal_lobbyist_name VARCHAR(255),                    -- Name of the principal lobbyist
    contractual_client_name VARCHAR(255),                    -- Name of the contractual client
    beneficial_client_name VARCHAR(255),                     -- Name of the beneficial client
    individual_lobbyist_name TEXT,                           -- Names of individual lobbyists involved
    compensation DECIMAL(15, 2),                             -- Compensation amount
    reimbursed_expenses DECIMAL(15, 2),                      -- Amount of reimbursed expenses
    expenses_less_than_75 DECIMAL(15, 2),                    -- Expenses less than $75
    lobbying_expenses_for_non DECIMAL(15, 2),                -- Expenses for non-lobbying activities
    itemized_expenses DECIMAL(15, 2),                        -- Total itemized expenses
    total_financials DECIMAL(15, 2),                         -- Total financial amount (calculated in ETL)
    expense_type VARCHAR(100),                               -- Type of expense
    expense_paid_to VARCHAR(255),                            -- Party the expense was paid to
    expense_reimbursed_by_client VARCHAR(255),               -- Client reimbursing the expense
    expense_purpose VARCHAR(255),                            -- Purpose of the expense
    expense_date DATE,                                       -- Date of the expense
    lobbying_subjects TEXT,                                  -- Subjects of lobbying
    level_of_government VARCHAR(50),                         -- Level of government involved (e.g., State)
    lobbying_focus_type VARCHAR(50),                         -- Type of lobbying focus
    focus_identifying_number VARCHAR(50),                    -- Identifying number of the focus area
    type_of_lobbying_communication VARCHAR(50),              -- Type of lobbying communication
    government_body VARCHAR(100),                            -- Government body involved
    monitoring_only BOOLEAN,                                 -- Monitoring-only indicator (Yes/No)
    party_name VARCHAR(255),                                 -- Name of the party involved
    unique_id VARCHAR(100),                                  -- Unique identifier for this record
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP           -- Timestamp for when the record was loaded
);
