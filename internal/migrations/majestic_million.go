package migrations

const INIT_MAJESTIC_MILLIONS = `
-- CREATE DATABASE IF NOT EXISTS majestic_millions;
-- USE majestic_millions;
CREATE TABLE IF NOT EXISTS domain (
        GlobalRank int,
        TldRank int,
        Domain varchar(255),
        TLD varchar(255),
        RefSubNets int,
        RefIPs int,
        IDN_Domain varchar(255),
        IDN_TLD varchar(255),
        PrevGlobalRank int,
        PrevTldRank int,
        PrevRefSubNets int,
        PrevRefIPs int
);
`
