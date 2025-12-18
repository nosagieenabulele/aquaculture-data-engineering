/********************************************************************************************
    FISH FARM DATABASE SCHEMA
    Purpose:
        Centralize operational data for ponds, batches, feeding, water quality, 
        biomass monitoring, inventory, and financial records.

    Design Notes:
        - All tables use strict constraints to ensure data quality.
        - Foreign keys enforce referential integrity.
        - Decimal scales are chosen to support biological precision.
        - Naming conventions follow enterprise standards (snake_case, singular tables).
********************************************************************************************/

-- ==========================================================================================
-- DATABASE CREATION
-- ==========================================================================================
DROP DATABASE IF EXISTS fishfarmdb;
CREATE DATABASE fishfarmdb;
USE fishfarmdb;

-- ==========================================================================================
-- CORE ENTITIES
-- ==========================================================================================

/*------------------------------------------------------------------------------
    Pond:
    Each physical pond where fish batches are stocked and grown.
------------------------------------------------------------------------------*/
CREATE TABLE pond (
    pond_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) UNIQUE NOT NULL,
    location VARCHAR(100),
    capacity INT NOT NULL,
    status ENUM('occupied','emptied') NOT NULL DEFAULT 'occupied'
);

/*------------------------------------------------------------------------------
    Batch:
    Represents a group of fish stocked and tracked together over lifecycle.
------------------------------------------------------------------------------*/
CREATE TABLE batch (
    batch_id INT PRIMARY KEY AUTO_INCREMENT,
    stock_date DATE NOT NULL,
    predicted_harvest DATE NOT NULL,
    initial_count INT NOT NULL CHECK(initial_count >= 0),
    initial_average DECIMAL(5,2) NOT NULL,
    weight DECIMAL(5,2) NOT NULL,
    status ENUM('active','completed','harvested') NOT NULL,
    current_count INT
);

-- ==========================================================================================
-- OPERATIONS TRACKING
-- ==========================================================================================

/*------------------------------------------------------------------------------
    Daily Records:
    Daily operational monitoring – feeding, temperature, mortality, behaviour.
------------------------------------------------------------------------------*/
CREATE TABLE daily_records (
    daily_record_id INT PRIMARY KEY AUTO_INCREMENT,
    record_date TIMESTAMP NOT NULL,
    temperature DECIMAL(5,2) NOT NULL,
    feed_eaten DECIMAL(10,5) NOT NULL,
    feed_size ENUM('2mm','3mm','4mm','6mm','9mm') NOT NULL,
    mortality INT NOT NULL CHECK(mortality >= 0),
    fish_behaviour ENUM('active','inactive') NOT NULL,
    notes VARCHAR(1000),
    pond_id INT NOT NULL,
    batch_id INT NOT NULL,
    FOREIGN KEY (pond_id) REFERENCES pond(pond_id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    FOREIGN KEY (batch_id) REFERENCES batch(batch_id)
        ON DELETE RESTRICT ON UPDATE CASCADE
);

CREATE INDEX idx_daily_record_pond ON daily_records(pond_id);
CREATE INDEX idx_daily_record_batch ON daily_records(batch_id);

/*------------------------------------------------------------------------------
    Weekly Check:
    Weight sampling and KPIs (SGR, FCR, biomass gain).
------------------------------------------------------------------------------*/
CREATE TABLE weekly_check (
    weekly_check_id INT PRIMARY KEY AUTO_INCREMENT,
    batch_id INT NOT NULL,
    week_no INT NOT NULL,
    record_date DATE,
    average_weight DECIMAL(10,5) NOT NULL,
    SGR DECIMAL(10,2) NOT NULL,
    FCR DECIMAL(10,2) NOT NULL,
    biomas DECIMAL(10,5),
    UNIQUE (batch_id, week_no),
    FOREIGN KEY (batch_id) REFERENCES batch(batch_id)
);

/*------------------------------------------------------------------------------
    Water Management:
    Water quality monitoring for each pond.
------------------------------------------------------------------------------*/
CREATE TABLE water_management (
    water_record_id INT PRIMARY KEY AUTO_INCREMENT,
    time_stamp TIMESTAMP NOT NULL,
    temperature DECIMAL(5,2) CHECK(temperature BETWEEN 20 AND 40),
    PH DECIMAL(5,2) CHECK(PH BETWEEN 5 AND 10),
    ammonia INT CHECK(ammonia >= 0),
    nitrate INT,
    nitrite INT,
    alkalinity INT,
    carbonate INT,
    total_hardness INT,
    water_change DATE,
    pond_id INT NOT NULL,
    FOREIGN KEY (pond_id) REFERENCES pond(pond_id)
);

-- ==========================================================================================
-- RESOURCE & COST MANAGEMENT
-- ==========================================================================================

/*------------------------------------------------------------------------------
    Inventory: Feed, medication, equipment.
------------------------------------------------------------------------------*/
CREATE TABLE inventory (
    inventory_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    category ENUM('feed', 'medication', 'equipment', 'unknown') NOT NULL,
    date_purchased DATE NOT NULL,
    quantity INT NOT NULL,
    cost INT NOT NULL,
    manufacturer VARCHAR(500)
);

/*------------------------------------------------------------------------------
    Expenses: All farm expenses with optional batch assignment.
------------------------------------------------------------------------------*/
CREATE TABLE expenses (
    expenses_id INT PRIMARY KEY AUTO_INCREMENT,
    purchase_date DATE NOT NULL,
    item VARCHAR(500) NOT NULL,
    category ENUM('feed','fuel','labour','medication','fish','transport','others') NOT NULL,
    quantity INT NOT NULL,
    cost INT NOT NULL,
    vendor VARCHAR(1000),
    batch_id INT,
    FOREIGN KEY (batch_id) REFERENCES batch(batch_id)
);

-- ==========================================================================================
-- RELATIONSHIP TABLES
-- ==========================================================================================

/*------------------------------------------------------------------------------
    Pond-Batch:
    Tracks which batch is assigned to which pond and when.
------------------------------------------------------------------------------*/
CREATE TABLE pond_batch (
    pond_batch_id INT PRIMARY KEY AUTO_INCREMENT,
    pond_id INT,
    batch_id INT,
    count_assigned INT,
    start_date DATE NOT NULL,
    end_date DATE,
    reason VARCHAR(1000),
    note VARCHAR(1000),
    FOREIGN KEY (pond_id) REFERENCES pond(pond_id),
    FOREIGN KEY (batch_id) REFERENCES batch(batch_id)
);

-- ==========================================================================================
-- KPI TARGETS
-- ==========================================================================================

/*------------------------------------------------------------------------------
    KPI Targets:
    Weekly expected growth benchmarks for performance comparison.
------------------------------------------------------------------------------*/
CREATE TABLE KPI_target (
    KPI_id INT PRIMARY KEY AUTO_INCREMENT,
    batch_id INT,
    record_date DATE NOT NULL,
    average_weight DECIMAL(10,5) NOT NULL,
    target_biomas DECIMAL(10,5) NOT NULL,
    weekly_gain DECIMAL(10,5) NOT NULL,
    FOREIGN KEY (batch_id) REFERENCES batch(batch_id)
);

-- ==========================================================================================
-- SAMPLE PRODUCTION DATA
-- ==========================================================================================
INSERT INTO pond (name, location, capacity, status) VALUES
('Pond A','North Zone',3000,'occupied'),
('Pond B','North Zone',2500,'occupied'),
('Pond C','South Zone',2000,'emptied');

INSERT INTO batch (stock_date,predicted_harvest,initial_count,initial_average,weight,status)
VALUES
('2025-01-10','2025-06-20',5000,0.02,0.02,'active'),
('2025-02-01','2025-07-15',3500,0.03,0.03,'active'),
('2025-03-05','2025-07-12',2000,0.04,0.04,'active');

