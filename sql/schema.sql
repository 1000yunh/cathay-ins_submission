-- ==========================================
-- RIS Scraper System - Database Schema
-- PostgreSQL 14+
-- ==========================================

-- Create Database (run manually if needed)
-- CREATE DATABASE ris_scraper;
-- \c ris_scraper;

-- Set timezone to Taiwan
SET timezone = 'Asia/Taipei';

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ==========================================
-- House Number Records Table
-- ==========================================
CREATE TABLE IF NOT EXISTS house_number_records (
    id SERIAL PRIMARY KEY,
    city VARCHAR(50) NOT NULL,
    district VARCHAR(50) NOT NULL,
    full_address VARCHAR(500),
    -- Parsed address fields
    village VARCHAR(50),           -- 里/村 (e.g., 富台里)
    neighborhood VARCHAR(20),      -- 鄰 (e.g., 019)
    road VARCHAR(200),
    section VARCHAR(50),
    lane VARCHAR(50),
    alley VARCHAR(50),
    number VARCHAR(50),
    floor VARCHAR(50),
    floor_dash VARCHAR(50),
    -- Assignment info
    assignment_type VARCHAR(50) NOT NULL,
    assignment_date DATE,
    assignment_date_roc VARCHAR(20),
    -- Metadata
    raw_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_house_record UNIQUE (city, district, full_address, assignment_date)
);

-- Indexes for house_number_records
CREATE INDEX IF NOT EXISTS idx_house_city_district ON house_number_records(city, district);
CREATE INDEX IF NOT EXISTS idx_house_assignment_date ON house_number_records(assignment_date);
CREATE INDEX IF NOT EXISTS idx_house_assignment_type ON house_number_records(assignment_type);
CREATE INDEX IF NOT EXISTS idx_house_created_at ON house_number_records(created_at);
CREATE INDEX IF NOT EXISTS idx_house_road ON house_number_records(road);
CREATE INDEX IF NOT EXISTS idx_house_full_address ON house_number_records(full_address);
CREATE INDEX IF NOT EXISTS idx_house_raw_data ON house_number_records USING GIN (raw_data);

-- ==========================================
-- Scraper Execution Logs Table
-- ==========================================
CREATE TABLE IF NOT EXISTS scraper_executions (
    id SERIAL PRIMARY KEY,
    execution_id UUID DEFAULT uuid_generate_v4(),
    city VARCHAR(50),
    district VARCHAR(50),
    query_params JSONB,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL')),
    records_count INTEGER DEFAULT 0,
    error_message TEXT,
    error_type VARCHAR(100),
    duration_seconds FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB
);

-- Indexes for scraper_executions
CREATE INDEX IF NOT EXISTS idx_scraper_execution_id ON scraper_executions(execution_id);
CREATE INDEX IF NOT EXISTS idx_scraper_city_district ON scraper_executions(city, district);
CREATE INDEX IF NOT EXISTS idx_scraper_status ON scraper_executions(status);
CREATE INDEX IF NOT EXISTS idx_scraper_start_time ON scraper_executions(start_time);

-- ==========================================
-- API Query Logs Table
-- ==========================================
CREATE TABLE IF NOT EXISTS api_query_logs (
    id SERIAL PRIMARY KEY,
    request_id UUID DEFAULT uuid_generate_v4(),
    endpoint VARCHAR(200) NOT NULL,
    method VARCHAR(10) NOT NULL,
    query_params JSONB,
    city VARCHAR(50),
    district VARCHAR(50),
    results_count INTEGER DEFAULT 0,
    response_time_ms FLOAT,
    status_code INTEGER,
    error_message TEXT,
    client_ip VARCHAR(50),
    user_agent TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for api_query_logs
CREATE INDEX IF NOT EXISTS idx_api_request_id ON api_query_logs(request_id);
CREATE INDEX IF NOT EXISTS idx_api_city_district ON api_query_logs(city, district);
CREATE INDEX IF NOT EXISTS idx_api_created_at ON api_query_logs(created_at);
CREATE INDEX IF NOT EXISTS idx_api_status_code ON api_query_logs(status_code);
CREATE INDEX IF NOT EXISTS idx_api_endpoint ON api_query_logs(endpoint);

-- ==========================================
-- Alert Notifications Table
-- ==========================================
CREATE TABLE IF NOT EXISTS alert_notifications (
    id SERIAL PRIMARY KEY,
    alert_id UUID DEFAULT uuid_generate_v4(),
    alert_type VARCHAR(50) NOT NULL CHECK (alert_type IN
        ('SCRAPER_ERROR', 'API_EMPTY_RESULT', 'DATABASE_ERROR', 'SYSTEM_ERROR')),
    severity VARCHAR(20) NOT NULL CHECK (severity IN
        ('INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    title VARCHAR(200) NOT NULL,
    message TEXT NOT NULL,
    metadata JSONB,
    notification_channels TEXT[],
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'SENT', 'FAILED')),
    retry_count INTEGER DEFAULT 0,
    error_message TEXT
);

-- Indexes for alert_notifications
CREATE INDEX IF NOT EXISTS idx_alert_alert_id ON alert_notifications(alert_id);
CREATE INDEX IF NOT EXISTS idx_alert_type ON alert_notifications(alert_type);
CREATE INDEX IF NOT EXISTS idx_alert_severity ON alert_notifications(severity);
CREATE INDEX IF NOT EXISTS idx_alert_sent_at ON alert_notifications(sent_at);
CREATE INDEX IF NOT EXISTS idx_alert_status ON alert_notifications(status);

-- ==========================================
-- System Logs Table (General Purpose)
-- ==========================================
CREATE TABLE IF NOT EXISTS system_logs (
    id SERIAL PRIMARY KEY,
    level VARCHAR(10) NOT NULL CHECK (level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    source VARCHAR(50) NOT NULL,  -- scraper, api, system, alert
    message TEXT NOT NULL,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for system_logs
CREATE INDEX IF NOT EXISTS idx_logs_source ON system_logs(source);
CREATE INDEX IF NOT EXISTS idx_logs_level ON system_logs(level);
CREATE INDEX IF NOT EXISTS idx_logs_created ON system_logs(created_at);
CREATE INDEX IF NOT EXISTS idx_logs_source_level ON system_logs(source, level);

-- ==========================================
-- Views for Analytics
-- ==========================================

-- Daily scraper statistics
CREATE OR REPLACE VIEW daily_scraper_stats AS
SELECT
    DATE(start_time) as date,
    city,
    district,
    COUNT(*) as total_executions,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_count,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_count,
    SUM(records_count) as total_records,
    AVG(duration_seconds) as avg_duration_seconds
FROM scraper_executions
GROUP BY DATE(start_time), city, district
ORDER BY date DESC;

-- API query statistics
CREATE OR REPLACE VIEW api_query_stats AS
SELECT
    DATE(created_at) as date,
    endpoint,
    COUNT(*) as total_queries,
    SUM(CASE WHEN status_code = 200 THEN 1 ELSE 0 END) as successful_queries,
    SUM(CASE WHEN results_count = 0 THEN 1 ELSE 0 END) as empty_results,
    AVG(response_time_ms) as avg_response_time_ms,
    MAX(response_time_ms) as max_response_time_ms
FROM api_query_logs
GROUP BY DATE(created_at), endpoint
ORDER BY date DESC;

-- Alert notification statistics
CREATE OR REPLACE VIEW alert_stats AS
SELECT
    DATE(sent_at) as date,
    alert_type,
    severity,
    COUNT(*) as total_alerts,
    SUM(CASE WHEN status = 'SENT' THEN 1 ELSE 0 END) as sent_count,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_count
FROM alert_notifications
GROUP BY DATE(sent_at), alert_type, severity
ORDER BY date DESC;

-- ==========================================
-- Triggers for auto-update timestamps
-- ==========================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger for house_number_records
CREATE TRIGGER update_house_number_records_updated_at
    BEFORE UPDATE ON house_number_records
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ==========================================
-- Functions for Data Management
-- ==========================================

-- Function to clean old logs (older than retention_days)
CREATE OR REPLACE FUNCTION clean_old_logs(retention_days INTEGER DEFAULT 365)
RETURNS TABLE(
    scraper_deleted INTEGER,
    api_deleted INTEGER,
    alert_deleted INTEGER,
    system_log_deleted INTEGER
) AS $$
DECLARE
    scraper_count INTEGER;
    api_count INTEGER;
    alert_count INTEGER;
    system_log_count INTEGER;
BEGIN
    -- Delete old scraper execution logs
    DELETE FROM scraper_executions
    WHERE created_at < CURRENT_DATE - retention_days * INTERVAL '1 day';
    GET DIAGNOSTICS scraper_count = ROW_COUNT;

    -- Delete old API query logs
    DELETE FROM api_query_logs
    WHERE created_at < CURRENT_DATE - retention_days * INTERVAL '1 day';
    GET DIAGNOSTICS api_count = ROW_COUNT;

    -- Delete old alert notifications
    DELETE FROM alert_notifications
    WHERE sent_at < CURRENT_DATE - retention_days * INTERVAL '1 day';
    GET DIAGNOSTICS alert_count = ROW_COUNT;

    -- Delete old system logs
    DELETE FROM system_logs
    WHERE created_at < CURRENT_DATE - retention_days * INTERVAL '1 day';
    GET DIAGNOSTICS system_log_count = ROW_COUNT;

    RETURN QUERY SELECT scraper_count, api_count, alert_count, system_log_count;
END;
$$ LANGUAGE plpgsql;

-- ==========================================
-- Sample Data (for testing)
-- ==========================================

-- INSERT INTO house_number_records (city, district, street_name, house_number, assignment_type, assignment_date, status)
-- VALUES
--     ('台北市', '大安區', '信義路', '123號', '門牌初編', '2025-09-15', '核准'),
--     ('台北市', '中正區', '忠孝東路', '456號', '門牌初編', '2025-10-20', '核准'),
--     ('台北市', '信義區', '基隆路', '789號', '門牌初編', '2025-11-10', '核准');

-- ==========================================
-- Permissions (Optional)
-- ==========================================

-- Create read-only user for API
-- CREATE USER api_user WITH PASSWORD 'api_secure_password';
-- GRANT CONNECT ON DATABASE ris_scraper TO api_user;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO api_user;
-- GRANT INSERT ON api_query_logs TO api_user;

-- ==========================================
-- Comments
-- ==========================================

COMMENT ON TABLE house_number_records IS 'Stores scraped house number registration data';
COMMENT ON TABLE scraper_executions IS 'Records scraper execution history';
COMMENT ON TABLE api_query_logs IS 'Records API query logs';
COMMENT ON TABLE alert_notifications IS 'Records alert notifications';
COMMENT ON TABLE system_logs IS 'General system logs (scraper, API, system events)';

COMMENT ON COLUMN house_number_records.raw_data IS 'JSON format storing original scraped data';
COMMENT ON COLUMN scraper_executions.execution_id IS 'Unique execution ID for tracking';
COMMENT ON COLUMN api_query_logs.response_time_ms IS 'API response time in milliseconds';
COMMENT ON COLUMN alert_notifications.notification_channels IS 'Notification channel list (email)';
