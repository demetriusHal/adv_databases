-- MySQL Script generated by MySQL Workbench
-- Mon Nov 25 12:57:32 2019
-- Model: New Model    Version: 1.0
-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema mydb
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema mydb
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS "mydb" DEFAULT CHARACTER SET utf8 ;
USE "mydb" ;

-- -----------------------------------------------------
-- Table "mydb"."log"
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS log (
  "id" SERIAL,
  "time" TIMESTAMP NULL,
  "source_ip" VARCHAR(32) NULL,
  "type" VARCHAR(32) NULL,
  PRIMARY KEY ("id"));


-- -----------------------------------------------------
-- Table "mydb"."access"
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS access (
  "user_id" VARCHAR(32) NULL,
  "http_method" VARCHAR(32) NULL,
  "resource" VARCHAR(256) NULL,
  "response" VARCHAR(32) NULL,
  "response_size" INT NULL,
  "referer" VARCHAR(128) NULL,
  "user_string" VARCHAR(256) NULL,
  "log_id" INT NOT NULL,
  PRIMARY KEY ("log_id"),
  CONSTRAINT "fk_access_log"
    FOREIGN KEY ("log_id")
    REFERENCES log ("id")
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)


-- -----------------------------------------------------
-- Table "mydb"."blocks"
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS blocks (
  "id" SERIAL,
  "log_id" INT NOT NULL,
  "dest_ip" VARCHAR(32) NULL,
  "block_requested" VARCHAR(32) NULL,
  "size" INT NULL,
  PRIMARY KEY ("id", "log_id"),
  CONSTRAINT "fk_rest_logs_log1"
    FOREIGN KEY ("log_id")
    REFERENCES log("id")
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)


-- -----------------------------------------------------
-- Table "mydb"."user"
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS my_user (
  "email" VARCHAR(45) NULL,
  "name" VARCHAR(45) NOT NULL,
  "password" VARCHAR(256) NULL,
  PRIMARY KEY ("name"))


-- -----------------------------------------------------
-- Table "mydb"."queries"
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS queries(
  "user_name" VARCHAR(45) NOT NULL,
  "time" TIMESTAMP NULL,
  "query" VARCHAR(128) NULL,
  PRIMARY KEY ("user_name"),
  CONSTRAINT "fk_table1_user1"
    FOREIGN KEY ("user_name")
    REFERENCES my_user("name")
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
