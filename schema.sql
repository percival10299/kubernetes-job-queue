CREATE TABLE jobs (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    status VARCHAR(16) NOT NULL,
    claimed_by VARCHAR(255) NULL,
    INDEX jobs_status_id (status, id)
) ENGINE=InnoDB;

INSERT INTO jobs (status) VALUES ('queued');
