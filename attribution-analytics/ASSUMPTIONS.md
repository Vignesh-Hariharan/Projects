# Project Assumptions

## Data Generation Assumptions

### User Behavior
- Users have 1-5 sessions on average, with most having 1-2 sessions
- Conversion rate is approximately 5% of total users
- Sessions occur during business hours (8am-10pm) weighted toward evening
- Session duration varies from 2-30 minutes
- Users interact with 3-8 pages per session

### Marketing Touchpoints
- 60% of web users are exposed to paid advertising
- Prospecting campaigns target users before their first website visit
- Retargeting campaigns target users after they've engaged with the site
- Ad impressions occur 1-14 days before first session (prospecting) or 1-7 days after (retargeting)
- 67% of ad impressions are viewable
- Click-through rate is approximately 0.8-1%

### Campaign Structure
- 12 campaigns total: 6 prospecting, 6 retargeting
- Three creative formats: display, video, native
- Campaigns run for the full date range (Nov 1 - Dec 15, 2024)
- Daily budgets vary by format: display ($500-750), video ($1200-1800), native ($800-1200)

## Attribution Assumptions

### Attribution Window
- 30-day lookback window from conversion
- Only touchpoints before conversion are included
- No post-conversion touchpoints considered

### Attribution Models
- **First Touch**: Assumes first interaction drives all value
- **Last Touch**: Assumes final interaction drives all value (industry baseline)
- **Linear**: Assumes equal contribution from all touchpoints
- **Position-Based (U-Shaped)**: 40% first, 40% last, 20% middle (assumes awareness and closing moments most important)

### Channel Classification
- Direct traffic: medium = '(none)'
- Organic search: medium = 'organic', source = search engine
- Social: medium = 'social', source = social platform
- Email: medium = 'email'
- Referral: medium = 'referral'
- Paid ads: campaign_type + creative_format (e.g., prospecting_display)

## Technical Assumptions

### Data Quality
- All timestamps are valid and in chronological order
- User IDs are consistent across sessions and impressions
- Revenue values are positive for all conversions
- No duplicate transaction IDs

### Snowflake Environment
- ACCOUNTADMIN role has full permissions
- COMPUTE_WH warehouse is available and running
- Database can be dropped and recreated cleanly
- X-Small warehouse is sufficient for this data volume

### dbt Assumptions
- dbt 1.7+ is compatible with Snowflake adapter
- Views are sufficient for staging/intermediate layers
- Tables are needed for final marts (query performance)
- Tests run after model builds

## Business Assumptions

### Marketing Strategy
- Prospecting campaigns focus on awareness and new customer acquisition
- Retargeting campaigns focus on conversion of engaged users
- Multi-touch journeys are common (average 3-5 touchpoints per conversion)
- Display ads drive awareness even if they don't directly lead to clicks

### Analysis Scope
- Focus is on paid media (display/video/native) vs organic channels
- Analysis timeframe is 45 days (sufficient for attribution analysis)
- B2C e-commerce context (single purchase per transaction)
- No consideration of offline touchpoints

## Limitations

1. **Synthetic Data**: Data is algorithmically generated, not real user behavior
2. **Simplified Journey**: Real customer journeys may be more complex
3. **Single Device**: Does not account for cross-device attribution
4. **No Seasonality**: Uniform distribution across date range
5. **No External Factors**: Weather, promotions, competitive activity not modeled
6. **Fixed Parameters**: Conversion rate, overlap, budgets are constants

## Why These Assumptions Matter

These assumptions enable:
- Reproducible results for portfolio demonstration
- Clear signal in attribution differences between models
- Realistic patterns that mirror actual marketing data
- Simplified analysis that highlights core attribution concepts

For production use, these would need validation against actual business data and refinement based on observed patterns.

