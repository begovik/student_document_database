-- 002: text_sample для LLM-класифікації
ALTER TABLE documents ADD COLUMN IF NOT EXISTS text_sample TEXT;