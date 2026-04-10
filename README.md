Cyclistic Bike Hire Analysis
Converting casual riders into annual members using data

Project Overview
This project addresses the challenge of converting casual bike hire users into annual members. By analysing usage patterns across rider types, time periods, rideable types, and locations, I developed data-driven recommendations to support long-term revenue growth.

Business Problem
Cyclistic wanted to understand how casual riders differ from annual members, and how those insights could be used to increase membership conversions without negatively impacting existing casual rider revenue.

Approach
 - Cleaned and analysed over 5 million bike hire records.
 - Engineered additional features including ride length, day of week, weekday/weekend classification, and hour of day.
 - Applied outlier handling (0.5–120 minutes) to improve statistical reliability and reduce skew, balancing business plausibility with statistical integrity.
 - Segmented behaviour by customer type, time of day, day of week, month, rideable type, and location.
 - Investigated station usage, including top locations and geographic spread.
 - Developed and evaluated targeted promotional strategies based on observed behavioural patterns.

Key insights
 - Members take significantly more rides, while casual riders take longer journeys on average.
 - Casual riders account for 13% more total ridden minutes, making them a critical revenue segment to retain.
 - Members show clear weekday and peak-hour usage patterns consistent with commuting behaviour.
 - Casual riders show stronger weekend usage and longer ride durations, indicating more leisure-oriented behaviour.
 - Both groups exhibit seasonal trends, with significantly higher usage in summer and sharp declines in winter.
 - Members use a slightly wider range of locations, while casual usage is more concentrated around popular leisure destinations.
 - Behavioural patterns differ between groups, suggesting different usage purposes, though with some overlap.

Recommendations
 - Introduce a targeted promotion encouraging casual riders to adopt commuting-style usage during weekday peak hours.
 - Incentivise this behaviour by linking it to discounted weekend leisure usage, preserving existing revenue streams.
 - Launch the promotion in June to maximise reach during peak casual usage periods.
 - Introduce a secondary winter-focused promotion (e.g. partnership with an outdoor clothing brand) to improve off-season retention, especially for newly converted Members.
 - Roll out promotions gradually and monitor closely to avoid negatively impacting casual rider revenue.

Tools and technologies
SQL (Google BigQuery, DuckDB), Tableau, Power BI, Excel.

Outputs
Included in this repository are:
 - A full stakeholder report, including supporting visualisations (Cyclistic_Project_Report.pdf)
 - SQL code (Cyclistic_SQL_Code.sql)
 - Power BI Dashboard, including the pbix file and an mp4 video demonstrating the dashboard in use. (/Power_BI_Dashboard/)
 - Aggregated data CSV files. (/Data_Aggregations/)

The raw original data can be found at the below link, as it is too big for GitHub:
https://drive.google.com/drive/folders/1BjjMS3ckG2cxFqTFX4H_yq3qp6DfhTyi?usp=sharing
