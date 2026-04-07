-- Migration: Update events table to include event_type
-- Date: 2024-01-02
-- Description: Adds event_type column if it doesn't exist

-- Check if event_type column exists, if not add it
ALTER TABLE analytics.events 
ADD COLUMN IF NOT EXISTS event_type String AFTER event_id;

-- Update existing rows to have default event_type
UPDATE analytics.events 
SET event_type = 'click' 
WHERE event_type = '';

-- Create index on event_type if it doesn't exist
ALTER TABLE analytics.events 
ADD INDEX IF NOT EXISTS idx_event_type event_type TYPE set(100) GRANULARITY 4;

