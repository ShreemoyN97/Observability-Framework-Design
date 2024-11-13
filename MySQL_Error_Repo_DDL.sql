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
