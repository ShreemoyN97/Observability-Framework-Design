CREATE DATABASE  IF NOT EXISTS `DAMG_7473_ADG_Framework_Database` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `DAMG_7473_ADG_Framework_Database`;

-- Create Pipeline_Observability table first
CREATE TABLE Pipeline_Observability (
    Processing_File_ID INT NOT NULL UNIQUE,
    File_Schema_ID INT NOT NULL,
    Time_Of_Arrival DATETIME NOT NULL,
    Process_StartTime DATETIME NOT NULL,
    Process_End_Time DATETIME NOT NULL,
    Input_File_Size VARCHAR(255) NOT NULL,
    Initial_Count_Of_Records INT NOT NULL,
    Count_Of_Processed_Records INT NOT NULL,
    Count_Of_Error_Records INT NOT NULL,
    Count_of_Distinct_Errors INT NOT NULL,
    PRIMARY KEY (Processing_File_ID)
);

-- Create File_Name table
CREATE TABLE File_Name (
    Processing_file_id INT AUTO_INCREMENT NOT NULL UNIQUE,
    Processing_file_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (Processing_file_id)
);

-- Create File_Schema table
CREATE TABLE File_Schema (
    File_Schema_ID INT AUTO_INCREMENT NOT NULL UNIQUE,
    File_Category_Name VARCHAR(255) NOT NULL,
    Schema_Text LONGBLOB NOT NULL,
    PRIMARY KEY (File_Schema_ID)
);

-- Create Error_Message_Reference table
CREATE TABLE Error_Message_Reference (
    Error_Code INT NOT NULL UNIQUE,
    Error_Message VARCHAR(255) NOT NULL,
    PRIMARY KEY (Error_Code)
);

-- Create Record_Error_Report table
CREATE TABLE Record_Error_Report (
    Record_ID INT AUTO_INCREMENT NOT NULL UNIQUE,
    Processing_File_ID INT NOT NULL,
    Record_Text LONGBLOB NOT NULL,
    PRIMARY KEY (Record_ID)
);

-- Create Column_Error_Report table
CREATE TABLE Column_Error_Report (
    Error_ID INT AUTO_INCREMENT NOT NULL UNIQUE,
    Record_ID INT NOT NULL,
    Processing_File_ID INT NOT NULL,
    Column_Name VARCHAR(255) NOT NULL,
    Error_Code INT NOT NULL,
    PRIMARY KEY (Error_ID)
);

-- Add foreign key constraints
ALTER TABLE Column_Error_Report
ADD CONSTRAINT Column_Error_Report_fk1 FOREIGN KEY (Record_ID) REFERENCES Record_Error_Report(Record_ID);

ALTER TABLE Column_Error_Report
ADD CONSTRAINT Column_Error_Report_fk4 FOREIGN KEY (Error_Code) REFERENCES Error_Message_Reference(Error_Code);

ALTER TABLE Record_Error_Report
ADD CONSTRAINT Record_Error_Report_fk1 FOREIGN KEY (Processing_File_ID) REFERENCES Pipeline_Observability(Processing_File_ID);

ALTER TABLE Pipeline_Observability
ADD CONSTRAINT Pipeline_Observability_fk0 FOREIGN KEY (Processing_File_ID) REFERENCES File_Name(Processing_file_id);

ALTER TABLE Pipeline_Observability
ADD CONSTRAINT Pipeline_Observability_fk1 FOREIGN KEY (File_Schema_ID) REFERENCES File_Schema(File_Schema_ID);

INSERT INTO Error_Message_Reference (Error_Code, Error_Message) VALUES
(1001, 'Missing required field'),
(1002, 'Null value encountered in non-nullable column'),
(1003, 'Data type mismatch'),
(1004, 'Value exceeds maximum allowable length'),
(1005, 'Invalid date format'),
(1006, 'Invalid numeric format'),
(1007, 'Foreign key constraint violation'),
(1008, 'Duplicate primary key'),
(1009, 'Unique constraint violation'),
(1010, 'Invalid value in enum field'),
(1011, 'Failed to parse JSON structure'),
(1012, 'Value out of allowable range'),
(1013, 'Inconsistent data between source and target'),
(1014, 'Missing foreign key reference'),
(1015, 'Duplicate record detected'),
(1016, 'Failed to connect to source system'),
(1017, 'Failed to connect to target database'),
(1018, 'Insufficient privileges for data access'),
(1019, 'Failed data validation against schema'),
(1020, 'Data truncation warning'),
(1021, 'Invalid currency code format'),
(1022, 'Failed data transformation'),
(1023, 'Record exceeds allowable file size'),
(1024, 'Unsupported file format'),
(1025, 'Required file missing in source'),
(1026, 'Row processing timeout'),
(1027, 'Header row missing in CSV file'),
(1028, 'Unsupported character encoding'),
(1029, 'Aggregation function error'),
(1030, 'Failed to load record into target database');

