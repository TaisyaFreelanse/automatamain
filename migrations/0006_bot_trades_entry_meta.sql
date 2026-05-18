-- V3 entry tape (buyer velocity, sell pressure, absorb, dumps, smart exits) for HISTORY UI.
ALTER TABLE bot_trades
    ADD COLUMN IF NOT EXISTS entry_meta text NOT NULL DEFAULT '';
