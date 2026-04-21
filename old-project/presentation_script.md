# Accelerating the Registrant Count: New Tools and Strategies for a One-Day Process

---

## Part 1: Evan's Rules for Hand Checking

The registrant count has always been constrained by hand checking. Over the past several years, we have steadily reduced the time required — from months of manual work, to a week with a full team, to four days with a small team. The next target is completing the entire process, end to end, within a single working day. The following principles, when fully implemented, constitute what I am calling "Evan's Rules" for hand checking. Individually, each rule produces meaningful efficiency gains. Together, they produce what I am calling "Evan's Law": a reliable 10x improvement in throughput. A project that takes ten hours becomes a one-hour project. A project that requires ten people can be completed by one person in the same amount of time.

This claim is grounded in measured results. The efficiency data is as follows.

**2024 Registrant Count:** 900 rows, 5 people, 1 week, 140 person-hours. Rate: 6.4 rows per person-hour.

**Original FPI Country Hand Checking:** 2,300 rows, 2 people, 1 week at 4 hours per day each, 40 person-hours. Rate: 57.5 rows per person-hour.

**Current FPI Hand Checking Process (Three Years of Data):** 600 rows, 1 person, 1 hour. Rate: 600 rows per person-hour.

That represents a 94x improvement over the registrant count rate and a 10.4x improvement over the original FPI process. At 600 rows per person-hour, a 900-row registrant count could be hand checked in roughly 90 minutes by a single analyst.

### Overarching Principle: Reduce Cognitive Load

Before walking through each rule, it is worth stating the principle that connects all of them. Every rule is designed to reduce the cognitive load of hand checking. Cognitive load is the mental effort required to complete a task. When cognitive load is high — frequent task switching, navigation, value lookup, uncertain progress — analysts fatigue quickly. Productive sessions become short bursts of 15 minutes or less, separated by breaks that may last anywhere from 15 minutes to a full day. When cognitive load is low — anomaly detection, minimal navigation, clear progress tracking — analysts can sustain productive sessions for one to two hours continuously. This compresses not only the per-filing time but the total project duration by eliminating lost time between sessions. Reducing cognitive load is as significant as reducing the actual seconds spent per filing.

---

### Rule 1: Sort by Similar Values

The order in which filings are checked is critically important. When filings are sorted so that similar values are grouped together, the task transforms from value lookup to anomaly detection. Instead of locating and verifying a specific value for each filing, the analyst scans a sequence of filings that are expected to share the same value. Attention shifts to identifying what is different rather than confirming what is correct. This eliminates copy-paste operations, reduces task switching, and leverages the brain's natural pattern recognition.

### Rule 2: Load Predictive Values

Rather than leaving fields blank for the analyst to fill in, pre-populate them with the best available predicted values. This converts the task from "look up and enter" to "confirm or correct." This rule is closely related to sorting: the better the predicted values, the finer the sorting, and the more the process collapses into pure anomaly scanning. While sorting on known metadata such as accelerated filer status or SIC code is helpful, sorting on predicted values for the actual column being checked is far more effective.

### Rule 3: Use Direct HTML Links

The standard filing URL loads the XBRL viewer, which must render the full filing with all tagged data. For large filings, this can take as long as three minutes per filing. Direct HTML links bypass the XBRL entirely and load the raw filing HTML in approximately one second. This is possible because each filing's HTML is a separate file within the full filing package, identifiable by its filename. I have developed a script that accepts any CSV of filing URLs, scrapes the top section of each filing's full-text index to extract the HTML filename, and outputs the original CSV with a new column containing the direct HTML URLs.

### Rule 4: Use HTML Fragments

When the relevant information is located in a specific section of the filing, an HTML fragment can be appended to the URL so that the browser jumps directly to that section on load. I have developed a process that scrapes for the fragment anchor rather than the value itself, which avoids the error-prone nature of value scraping. This eliminates the need to navigate tables of contents or scroll through documents, which preserves the flow state required for fast anomaly detection. Any interruption to navigate within a filing reintroduces task switching and increases cognitive load.

### Rule 5: Batch Openings

Rather than copying and pasting URLs one at a time into a browser, I have developed an Excel macro that takes a highlighted column of URLs and opens every one of them simultaneously in a new browser window, in order from top to bottom. The tabs appear left to right in the same sequence as the spreadsheet rows. Combined with Ctrl+Tab and Ctrl+Shift+Tab to navigate between tabs, this allows the analyst to move from filing to filing almost instantaneously. This works reliably for up to approximately 100 tabs before performance degrades. The synergy with the previous rules is significant: because each tab loads a direct HTML link with a fragment, every tab opens immediately to the correct section of the filing, and the analyst can hold down Ctrl+Tab and scan through filings at a pace limited only by pattern recognition speed.

### Rule 6: Use TODO and SKIP Placeholders

Instead of leaving unchecked fields blank, fill them with standardized placeholder values: TODO for fields that need to be checked, and SKIP for fields that are not required. This serves two purposes. First, it allows the analyst to work within the full dataset without needing to subset the data, since blank values are eliminated. Second, it enables a live progress counter. A formula in the frozen header row counts the remaining TODO values across all relevant columns. The analyst begins the session with a known count and watches it decrease with every filing submitted. This leverages an established psychological principle: the shorter the gap between effort and feedback, the more sustainable the effort becomes. It is the same principle that makes video games engaging for extended sessions — constant, immediate feedback on progress. Without a counter, analysts waste time checking how much remains and are more susceptible to fatigue. With a counter, the work becomes self-reinforcing, and sustained sessions are significantly easier to maintain.

---

### Evan's Law: Summary

When all six rules are implemented together, the compounding effect reliably produces at least a 10x improvement in hand-checking throughput. This is not theoretical — it is demonstrated by the measured progression from 6.4 rows per person-hour to 600 rows per person-hour, a 94x gain. Even accounting for differences in task complexity across projects, a 10x improvement is a conservative, defensible claim.

---

### A Note on Jevons Paradox

There is a well-documented economic principle known as Jevons Paradox: when efficiency improvements reduce the cost of an activity, the typical result is not that you do the same amount in less time — it is that you do far more. This is worth being mindful of. The goal of these improvements is not to hand check more things simply because we can. We should remain targeted in what we choose to verify and continue to rely on heuristics where they are sufficient. The real value of these efficiency gains is releasing constraints. Hand checking has dictated when we do projects, who is available to do them, and how ambitious we can be in scope. It has required intern availability, delayed timelines by weeks or months, and made quarterly production infeasible. If the constraint is removed, the strategic possibilities open up significantly.

---

## Part 2: Power BI Data Access Tool

The registrant count dataset is used by economists across the office for economic analysis supporting rulemaking. Previously, the data was distributed as a CSV on a shared drive, with manual backups maintained against accidental edits or deletions. Analysts routinely received ad hoc requests to produce tables, breakdowns, and visualizations from the data — work that was often duplicated across multiple requests.

Working with a contractor from the Power BI team, I have developed an internal Power BI tool that addresses these issues. The tool loads the registrant count dataset on the back end and automatically generates a set of pre-built tables and visualizations based on the types of requests we have historically received. The data is read-only within the tool — users can filter and explore but cannot alter the underlying dataset. Filtered subsets can be exported to Excel or CSV at any time.

### Current Capabilities

The tool provides centralized, read-only access to the registrant count data with controlled permissions. Pre-built tables and visualizations eliminate duplicated work across the office. Users can filter the dataset to their needs and export subsets without risk to the source data.

### Expansion Opportunities

**Scheduled refresh pipeline.** Connect Power BI directly to the data source so that quarterly uploads automatically refresh all dashboards without manual intervention.

**Tiered access with separate workspaces.** Maintain a full-detail internal workspace for our office and a curated external workspace for the Division of Corporation Finance, the Office of International Affairs, and other requesting offices — different views built on the same underlying data.

**Row-level security.** Control what specific divisions or users can see within a single workspace, without maintaining separate files or tools.

**Subscription alerts.** Economists and other stakeholders can subscribe to receive an emailed PDF or Excel snapshot automatically when the data refreshes — eliminating the need for a request entirely.

**Natural language Q&A.** Power BI includes a built-in feature that allows users to type questions such as "how many accelerated filers in Q3" and receive an auto-generated visualization. This could significantly reduce ad hoc data requests.

**Historical trend dashboards.** As quarterly data accumulates, cross-quarter and cross-year trend views become possible — analysis that is impractical with one-off CSV snapshots.

**Direct Excel connection.** Power BI datasets can be connected to directly from Excel via "Get Data," allowing economists to work in their preferred environment with always-current data and no file transfers.

---

## The Path to Quarterly Production

The combination of Evan's Rules and the Power BI tool makes a one-day end-to-end process realistic. At measured rates, the hand-checking component for a quarterly payload would take a single analyst roughly one to two hours. With the Power BI tool in place, the data can be uploaded and made available — with all visualizations and tables refreshed — on the same day. This means that on the first working day after a quarter closes, the registrant count for that quarter could be completed and accessible to the entire office. This has never been feasible before. It is feasible now.
