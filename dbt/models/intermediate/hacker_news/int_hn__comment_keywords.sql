{{
    config(
        materialized='table',
        schema='hacker_news',
        tags=['hacker_news']
    )
}}

with comments as (
    select * from {{ ref('stg_hn__comments') }}
),

stories as (
    select * from {{ ref('stg_hn__stories') }}
),

-- Define keywords to track with regex patterns
-- Prefix matches (no trailing \b) for terms with common variants (GPT-4, Claude-3, etc.)
-- Exact matches (with trailing \b) for generic terms that would over-match
keywords as (
    select keyword, pattern from unnest([
        -- AI/ML Core (prefix matches for version variants)
        struct('AI' as keyword, r'\bai\b|\bartificial intelligence' as pattern),  -- exact: avoid "air", "aim"
        struct('LLM', r'\bllm'),  -- matches llm, llms, llm-based
        struct('GPT', r'\bgpt'),  -- matches gpt, gpt-4, gpt4, gpt-4o, gpt5
        struct('ChatGPT', r'\bchatgpt'),  -- matches chatgpt, chatgpt-4
        struct('OpenAI', r'\bopenai'),  -- matches openai, openai's
        struct('Claude', r'\bclaude'),  -- matches claude, claude-3, claude-sonnet
        struct('Anthropic', r'\banthropic'),
        struct('Gemini', r'\bgemini'),  -- matches gemini, gemini-pro, gemini-1.5
        -- AI Ecosystem & Trends
        struct('AI Agents', r'\bai agents?|\bagents?\b'),
        struct('MCP', r'\bmcp\b|\bmodel context protocol'),
        struct('Agentic', r'\bagentic'),
        struct('Vibe Coding', r'\bvibe ?coding|\bvibecoding'),
        struct('AI Bubble', r'\bai bubble'),
        struct('AI Slop', r'\bai slop|\bslop\b'),
        -- Big Tech Companies
        struct('Google', r'\bgoogle'),
        struct('Apple', r'\bapple\b'),  -- exact: avoid "pineapple"
        struct('Microsoft', r'\bmicrosoft'),
        struct('Nvidia', r'\bnvidia'),
        struct('Amazon', r'\bamazon|\baws\b'),
        struct('Meta', r'\bmeta\b|\bfacebook'),  -- exact meta: avoid "metadata"
        -- Tech Leaders (last names)
        struct('Musk', r'\bmusk'),
        struct('Altman', r'\baltman'),
        struct('Zuckerberg', r'\bzuckerberg'),
        struct('Pichai', r'\bpichai'),
        struct('Huang', r'\bhuang'),
        struct('Nadella', r'\bnadella'),
        struct('Hassabis', r'\bhassabis'),
        struct('Amodei', r'\bamodei'),
        -- Programming Languages
        struct('Rust', r'\brust\b'),  -- exact: avoid "frustrated", "rusty"
        struct('Python', r'\bpython'),  -- matches python, python3, pythonic
        struct('JavaScript', r'\bjavascript|\bjs\b'),
        -- Platforms & Systems
        struct('Linux', r'\blinux'),
        struct('Windows', r'\bwindows'),
        struct('Browser', r'\bbrowser|\bchrome|\bfirefox|\bsafari'),
        -- Crypto
        struct('Crypto', r'\bcrypto'),  -- matches crypto, cryptocurrency
        struct('Bitcoin', r'\bbitcoin|\bbtc\b'),
        -- Hiring/Jobs
        struct('Hiring', r'\bhiring|\bjobs?\b'),
        struct('Interview', r'\binterview'),
        struct('Layoffs', r'\blayoff|\blaid off'),
        -- Software Engineering
        struct('Software Engineer', r'\bsoftware engineer|\bswe\b'),
        struct('Developer', r'\bdeveloper|\bdev\b'),
        struct('Engineering', r'\bengineering'),
        struct('Tech Industry', r'\btech industry|\bbig tech|\bfaang'),
        -- Tech Topics
        struct('Security', r'\bsecurity|\bcybersecurity|\bvulnerabilit'),
        struct('Quantum', r'\bquantum'),
        struct('Self Hosted', r'\bself[- ]?hosted'),
        -- Other high-engagement topics
        struct('Open Source', r'\bopen[- ]?source'),
        struct('Remote Work', r'\bremote work|\bwork from home|\bwfh\b'),
        struct('Startup', r'\bstartup')
    ])
),

-- Match comments to keywords via their parent story
comment_keywords as (
    select
        c.comment_id,
        c.story_id,
        c.comment_text_clean,
        c.posted_day,
        k.keyword
    from comments c
    join stories s on c.story_id = s.story_id
    cross join keywords k
    where regexp_contains(lower(s.title), k.pattern)
)

select * from comment_keywords
