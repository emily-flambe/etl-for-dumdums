with stories as (
    select * from {{ ref('stg_hn__stories') }}
),

-- Define keywords to track with their regex patterns
-- Using word boundaries (\b) to avoid partial matches (e.g., "fair" matching "ai")
-- Special characters like + need escaping in regex
keywords as (
    select keyword, pattern from unnest([
        -- Languages
        struct('Python' as keyword, r'\bpython\b' as pattern),
        struct('JavaScript', r'\bjavascript\b'),
        struct('Rust', r'\brust\b'),
        struct('Go', r'\bgolang\b|\bgo\s+lang'),  -- "Go" alone is too common
        struct('TypeScript', r'\btypescript\b'),
        struct('Java', r'\bjava\b'),
        struct('C++', r'\bc\+\+\b'),  -- Escape the plus signs
        struct('Ruby', r'\bruby\b'),
        struct('Swift', r'\bswift\b'),
        struct('Kotlin', r'\bkotlin\b'),
        -- Frameworks/Tools
        struct('React', r'\breact\b'),
        struct('Vue', r'\bvue\.?js\b|\bvuejs\b'),
        struct('Next.js', r'\bnext\.?js\b'),
        struct('Node', r'\bnode\.?js\b|\bnodejs\b'),
        struct('Django', r'\bdjango\b'),
        struct('Rails', r'\brails\b'),
        -- AI/ML
        struct('AI', r'\bai\b|\bartificial intelligence\b'),
        struct('LLM', r'\bllm\b|\blarge language model'),
        struct('GPT', r'\bgpt\b'),
        struct('ChatGPT', r'\bchatgpt\b'),
        struct('OpenAI', r'\bopenai\b'),
        struct('Claude', r'\bclaude\b'),
        struct('Anthropic', r'\banthropic\b'),
        struct('Machine Learning', r'\bmachine learning\b|\bml\b'),
        -- Infrastructure
        struct('Kubernetes', r'\bkubernetes\b|\bk8s\b'),
        struct('Docker', r'\bdocker\b'),
        struct('AWS', r'\baws\b|\bamazon web services\b'),
        struct('Azure', r'\bazure\b'),
        struct('Cloud', r'\bcloud\b'),
        -- Trends
        struct('Crypto', r'\bcrypto\b|\bcryptocurrency\b'),
        struct('Blockchain', r'\bblockchain\b'),
        struct('Bitcoin', r'\bbitcoin\b|\bbtc\b'),
        struct('Startup', r'\bstartup\b'),
        struct('Remote Work', r'\bremote work\b|\bwork from home\b|\bwfh\b'),
        struct('Open Source', r'\bopen source\b|\bopen-source\b'),
        -- Security
        struct('Security', r'\bsecurity\b'),
        struct('Privacy', r'\bprivacy\b'),
        struct('Encryption', r'\bencryption\b'),
        struct('Hack', r'\bhack\b|\bhacker\b')
    ])
),

-- Count mentions per week per keyword
keyword_matches as (
    select
        s.posted_week as week,
        k.keyword,
        count(*) as mention_count,
        sum(s.score) as total_score,
        round(avg(s.score), 1) as avg_score
    from stories s
    cross join keywords k
    where s.posted_week is not null
      and regexp_contains(lower(s.title), k.pattern)
    group by s.posted_week, k.keyword
)

select * from keyword_matches
order by week desc, mention_count desc
