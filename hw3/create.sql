-- drop schema if exists CSVCatalog;
-- CREATE SCHEMA `CSVCatalog` ;
-- use CSVCatalog ;

-- drop table if exists catalog_tables;
-- CREATE TABLE `catalog_tables` (
--   `table_name` varchar(32) NOT NULL,
--   `data_path` text,
--   PRIMARY KEY (`table_name`));

-- drop table if exists catalog_columns;
-- CREATE TABLE `CSVCatalog`.`catalog_columns` (
--   `table_name` VARCHAR(45) NOT NULL,
--   `column_name` VARCHAR(45) NOT NULL,
--   `is_nullable` ENUM('yes', 'no') NOT NULL,
--   `type` ENUM('text', 'number') NOT NULL,
--   PRIMARY KEY (`table_name`, `column_name`));

-- drop table if exists catalog_indices;
-- CREATE TABLE `CSVCatalog`.`catalog_indices` (
--   `index_name` VARCHAR(45) NOT NULL,
--   `table_name` VARCHAR(45) NOT NULL,
--   `column_name` VARCHAR(45) NOT NULL,
--   `ordinal_position` INT NOT NULL,
--   `index_type` ENUM('PRIMARY', 'UNIQUE', 'INDEX') NULL,
--   PRIMARY KEY (`index_name`, `ordinal_position`, `column_name`, `table_name`));
--  


## PRIMARY KEY
## catalog_tables - table_name
## 