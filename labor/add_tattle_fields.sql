-- Add new customer & engagement fields to tattle_reviews table
-- Run this in the Supabase SQL editor before running the refresh workflow

ALTER TABLE tattle_reviews ADD COLUMN IF NOT EXISTS customer_email TEXT;
ALTER TABLE tattle_reviews ADD COLUMN IF NOT EXISTS share_email BOOLEAN;
ALTER TABLE tattle_reviews ADD COLUMN IF NOT EXISTS customer_id BIGINT;
ALTER TABLE tattle_reviews ADD COLUMN IF NOT EXISTS customer_first_name TEXT;
ALTER TABLE tattle_reviews ADD COLUMN IF NOT EXISTS customer_last_name TEXT;
ALTER TABLE tattle_reviews ADD COLUMN IF NOT EXISTS incident_id BIGINT;
ALTER TABLE tattle_reviews ADD COLUMN IF NOT EXISTS message_count INTEGER DEFAULT 0;
ALTER TABLE tattle_reviews ADD COLUMN IF NOT EXISTS has_unread_messages BOOLEAN DEFAULT FALSE;
ALTER TABLE tattle_reviews ADD COLUMN IF NOT EXISTS tag_labels JSONB;
ALTER TABLE tattle_reviews ADD COLUMN IF NOT EXISTS reward_redeemed NUMERIC DEFAULT 0;

-- Index on customer_email for marketing queries
CREATE INDEX IF NOT EXISTS idx_tattle_customer_email ON tattle_reviews(customer_email)
  WHERE customer_email IS NOT NULL AND share_email = TRUE;
