# Learning pyramid: from lending money to understanding TLAC research

Created 22 September 2026 for a guided detour through the research already completed. New investigation is paused. This is a learning map, not an assignment to master all these terms before we talk.

The starting point is no assumed knowledge of bonds or bank resolution. We will use short explanations, one example at a time, and one question to check understanding. The detailed findings and their sources remain in [RESULTS.md](RESULTS.md). Numbers below are findings already discussed, not new estimates or updates.

## The shape of the pyramid

Read this picture **from the bottom upward**. The detailed lessons below then follow that learning order, starting at Tier 1.

```text
                             / 10. Our findings and their limits \
                          / 9. How we build and verify evidence     \
                       / 8. Cross-border contracts and execution       \
                    / 7. What an ownership percentage actually measures   \
                 / 6. Who owns, manages and holds a security                  \
              / 5. TLAC: which resources count, where and when                   \
           / 4. Bank failure, loss absorption and bail-in                           \
        / 3. Banks, balance sheets and the order of losses                             \
     / 2. Bonds: promises, prices, quantities and time                                     \
  / 1. Money, lending, ownership, claims and percentages                                      \
```

This is a dependency map, not a claim that every lesson depends on every earlier lesson. Ownership in Tier 6 builds mainly on Tiers 1–2; bank resolution in Tiers 4–5 builds mainly on Tier 3. Those paths meet when we ask **who would bear losses on a particular bank's particular securities**.

Our recurring example will be a small imaginary business, then an imaginary bank, and finally the actual HSBC calculation. Keeping the example familiar should let each new idea do one job.

## Tier 1 — What does it mean to lend money or own a business?

**Central idea:** Two people can give the same company money and receive very different rights in return.

Learn in this order:

1. A company is a legal entity that can own things, owe money and enter agreements.
2. A lender gives money in exchange for a promise of repayment. The company has a debt; the lender has a claim against it.
3. A shareholder owns a share of the business. Ordinary shares generally do not promise repayment of a fixed amount on a fixed date.
4. An investor gives up money now in the hope of receiving value later. A promised payment and a guaranteed payment are different things.
5. The same relationship looks different from opposite sides: the company's debt is an asset of its lender.
6. A percentage always has a numerator, meaning the part being measured, and a denominator, meaning the whole it is compared with. Both need compatible units.
7. A count and an amount answer different questions. Owning one of ten bond issues need not mean owning 10% of the borrowed money.

**Starting example:** A bakery obtains $100 from an owner and borrows $100 from a lender. Both supply funding; only one receives a repayment promise. We first understand those two relationships before adding interest, trading or bank rules.

**Ready to move on when:** You can explain the difference between lending and owning in your own words, from both the investor's and company's perspectives.

## Tier 2 — What is a bond, and what does its “value” mean?

**Central idea:** A bond packages a borrowing arrangement into a security that investors can hold and often trade. Its repayment amount and trading price are different quantities.

Learn in this order:

1. **Issuer and holder:** the entity making the promise and the investor holding the claim.
2. **Principal, face value, par and nominal amount:** closely related terms for the stated amount of the claim in our examples. Read each dataset's definition rather than assuming all amount fields are identical.
3. **Coupon and interest:** the payment terms; fixed versus floating payments. A quoted annual rate is not itself a dollar payment.
4. **Maturity:** the date principal is scheduled to be repaid. **Perpetual** securities have no fixed maturity. **Calls** allow early redemption under specified conditions; a first call date is not automatically a repayment date.
5. **Primary issuance versus secondary trading:** buying a newly issued bond funds the issuer; buying an existing bond generally pays its previous holder.
6. **Price and market value:** $1,000 of principal can trade for $950. Selling at that price does not by itself reduce the issuer's contractual $1,000 principal obligation.
7. **Yield:** a return measure incorporating price and promised payments. Understand why price and yield can move in opposite directions; no bond-pricing mathematics is required for the pilot.
8. **Credit risk and ratings:** the possibility of nonpayment and assessments of that risk. A rating is not a guarantee.
9. **Currency:** a dollar bond describes its denomination, not the residence of its owner. Currency translation changes the reporting unit, not the security's identity.
10. **Stock versus flow:** outstanding principal is measured at a date; issuance is measured over a period. Repayment, calls and buybacks can reduce the stock while new issuance remains positive.
11. **Issue, series, tranche and tap:** one financing can contain multiple separately specified securities; a later addition can increase an existing series. A count of identifiers, dates or transactions need not match.
12. **Dates:** announcement, issue, settlement, maturity, holdings date and filing date serve different purposes.

**Practice:** Follow a $1,000 bond from issuance to resale to repayment. Track who pays whom and distinguish principal from cash changing hands.

**Ready to move on when:** You can explain how a bank can issue new bonds during a quarter while its outstanding debt falls.

## Tier 3 — How does a bank's balance sheet work, and who takes a loss first?

**Central idea:** A bank's assets and the claims used to finance them are different sides of the same business.

Learn in this order:

1. **Assets, liabilities and equity:** assets are resources and claims the bank owns; liabilities are obligations; accounting equity is the residual difference. Assets = liabilities + equity.
2. A customer loan is an asset of the bank. A customer's deposit is a liability of the bank. Deposits, bonds and ordinary equity are different funding sources.
3. An asset loss initially reduces equity. A simplified bank with $100 of assets, $90 of liabilities and $10 of equity has $4 of equity after a $6 asset loss, before other changes.
4. **Capital is not a separate jar of cash.** Funding, accounting equity, regulatory capital and readily available cash are related but distinct.
5. **Liquidity versus solvency:** inability to pay today and insufficient asset value to cover obligations are different problems, though they can interact.
6. **Creditor ranking:** secured versus unsecured claims, senior versus subordinated claims, and why shareholders generally stand behind creditors. Actual priorities depend on the applicable rules and instrument.
7. **A banking group versus a legal issuer:** a parent holding company and its operating-bank subsidiaries have separate obligations. A brand name is not an adequate description of who owes the money.
8. **Consolidation and internal positions:** a parent lending to a subsidiary creates claims inside the group. Adding every entity's reported total can count the same funding twice.

**Practice:** Draw a bank's two sides and trace an asset loss into equity. Then distinguish the parent company's bond from its subsidiary bank's deposit.

**Ready to move on when:** You can explain why “the bank lost $6” does not necessarily mean it immediately failed to pay a bondholder $6.

## Tier 4 — What problem do resolution and bail-in solve?

**Central idea:** When a major bank fails, authorities may try to allocate losses and restore its financial strength while keeping essential activities working.

Learn in this order:

1. **Systemic importance:** a bank's failure can affect payment services, lending and other institutions. A **G-SIB** is a globally systemically important bank; the designation concerns consequences of failure, not immunity from failure.
2. **Resolution:** a special framework for dealing with a failing bank. Distinguish it from ordinary liquidation and from an assumption that every activity must continue unchanged.
3. **Bail-out and bail-in:** public support versus imposing losses on shareholders and creditors through the relevant mechanisms. Real cases can involve several tools.
4. **Write-down:** reducing a claim. **Conversion:** replacing a debt claim with an equity claim. Receiving shares does not guarantee recovery of the old debt's value.
5. **Loss absorption versus recapitalization:** covering losses and leaving the reorganized bank with a usable equity cushion are related but separate needs.
6. A debt-to-equity conversion changes claims and the balance sheet; it does not by itself deliver new cash to the bank. Liquidity can remain a separate problem.
7. **Contractual trigger versus statutory power:** a contract can prescribe conversion after a specified event; legislation can authorize an authority to impose conversion. These are not interchangeable.
8. **Execution:** deciding which claims change is only the beginning. Records, notices, trading arrangements and delivery of replacement securities must also work.

**Practice:** Use a simplified balance sheet to compare reducing $10 of debt with converting $10 of debt into shares. Separate the accounting effect from the investor's eventual recovery.

**Ready to move on when:** You can explain why converting creditors into shareholders may help a bank continue without making those creditors whole.

## Tier 5 — What is TLAC, and what counts toward it?

**Central idea:** Total loss-absorbing capacity, or **TLAC**, is a regulatory framework for having resources available to absorb losses and recapitalize a failing bank. It is not one uniform product called a “TLAC bond.”

Learn in this order:

1. **Requirement versus resources:** the minimum a bank must maintain differs from what it actually has.
2. **CET1, AT1 and Tier 2:** Common Equity Tier 1, Additional Tier 1 and Tier 2 are regulatory capital categories. Start with ordinary equity, then learn the additional instruments and conditions separately. CET1 includes retained earnings and regulatory adjustments; it is not the share-market value of the bank.
3. **Other eligible liabilities:** certain debt outside regulatory capital can count toward TLAC. Eligibility requires more than belonging to a large bank.
4. **Maturity and recognition:** a security can still exist and be owed to investors while its recognized contribution changes or ends. Full principal and recognized regulatory amount can differ.
5. **External versus internal TLAC:** funding from outside a resolution group versus loss-absorbing arrangements within a group. Learn the reporting boundary before adding amounts.
6. **Resolution entity and resolution group:** the entity and group boundary relevant to a resolution plan need not equal every company sharing a commercial brand.
7. **MREL:** the minimum requirement for own funds and eligible liabilities is a related European/UK framework covering a broader bank population. An MREL total is not automatically a global G-SIB TLAC total.
8. **Three routes to subordination:** contractual ranking; ranking established by law; and structural subordination arising from the issuer's position in a group.
9. **Three separate classifications:** legal form, accounting treatment and regulatory category. A security can have equity accounting treatment yet have contractual terms unlike ordinary shares.
10. **Convertible securities:** an ordinary investor-option convertible, a contingent convertible capital instrument and senior debt convertible under statutory bail-in powers are different mechanisms.
11. **Ratios:** risk-weighted assets and leverage exposure are different denominators. A TLAC regulatory ratio is not a percentage of bonds owned by investors.
12. **Institutions and documents:** the FSB develops international standards; national or regional authorities implement applicable regimes; banks' Pillar 3 and instrument disclosures report relevant amounts and terms.

**Ready to move on when:** You can explain why a list of a parent's bonds need not sum to its banking group's reported TLAC resources.

## Tier 6 — Who “owns” a bond when funds and intermediaries are involved?

**Central idea:** The name on an account, the manager choosing investments, the fund holding a bond and the people bearing the ultimate gains and losses can all differ.

Learn in this order:

1. Direct holdings by households, insurers, pension plans, banks and other organizations.
2. A mutual fund or ETF holds investments, while its investors own shares in the fund. A manager makes investment decisions but is not necessarily the economic owner.
3. Separate accounts, pooled funds, private funds and insurance accounts can create different ownership chains.
4. Brokers, custodians, nominees and securities depositories help hold records and move securities. Seeing an intermediary's name does not establish the ultimate investor's residence.
5. **Domicile, residence, address and headquarters:** these are different facts. A US manager can manage a foreign fund; a foreign resident can invest in a US fund.
6. **Look-through:** tracing through a fund to its investors. It requires another dataset beyond the fund's bond holdings.
7. **Gross versus net exposure:** holding a bond is not the same as bearing all its risk after hedges. Credit protection or other offsetting positions can change losses.
8. **Securities lending and collateral:** a reported investment on loan and a security received as collateral require different treatment. Avoid counting one economic position twice.
9. **Observed ownership versus complete ownership:** disclosure rules reveal some investors more reliably than others. No record found is not evidence of no owner.

**Practice:** Trace a US household → foreign fund → UK bank bond chain, alongside the US manager and custodian. Identify whose country each label describes.

**Ready to move on when:** You can explain why “held by a US-address fund” does not establish “ultimately owned by US residents.”

## Tier 7 — What exactly are we calculating?

**Central idea:** Before dividing two numbers, specify the question so precisely that someone else can identify the same numerator and denominator.

Learn in this order:

1. Fix the bank population, legal issuers, instrument population, investor population and measurement date.
2. Choose principal, market value or recognized regulatory resources. Do not divide fund market values by regulatory TLAC and call the result an ownership percentage.
3. Distinguish “US share of observed holdings” from “US holdings divided by all securities outstanding.” Missing investors affect these differently.
4. Use compatible currencies and dates. Issuance-date exchange rates and quarter-end exchange rates answer different questions.
5. Keep unmatched securities in a full-inventory denominator. Otherwise the ownership result can rise merely because coverage gets worse.
6. Separate missing values from zero; descriptive observations from statistical estimates; and an assumed lower bound from a proven one.
7. An aggregate ownership fraction is amount-weighted: add compatible holdings and divide by compatible outstanding amounts. Do not simply average bond-level percentages when bond sizes differ.
8. Distinguish total nominal securities, total market value and regulatory resources, especially when ordinary equity is included.
9. For annual issuance, define whether counting deals, tranches, security series or additional issuances. A current inventory omits issues that have disappeared: this is a form of survivorship bias.
10. Missing coverage is not necessarily random. A dataset rich in funds and sparse in households cannot be assumed representative of all investors.

**Practice:** Explain why a 12% US share of a sample of fund holdings need not mean US investors own 12% of all outstanding bonds.

**Ready to move on when:** You can complete: “This percentage measures ___ held by ___, divided by ___, on ___; it does not establish ___.”

## Tier 8 — Why do contracts, countries and US securities law matter?

**Central idea:** The security's promise, the authority's powers and the process for delivering a replacement claim all matter. Investor geography alone cannot describe the execution problem.

Learn in this order:

1. **Prospectus, indenture, supplement and final terms:** documents that establish or describe a programme and an individual security's rights. A programme description does not prove that every issue has every feature.
2. Coupon cancellation, cumulative versus non-cumulative payments, calls, perpetual terms, unpaid interest and ranking.
3. Capital-ratio triggers, non-viability events and resolution decisions. Who decides that a trigger occurred, and what discretion remains?
4. Full or partial conversion, permanent or temporary write-down, conversion-price formulas, price floors and the number and issuer of new shares.
5. Home country, issuer country, governing law, offering jurisdiction, currency and holder residence are separate attributes.
6. **US securities registration:** an initial offering, a later exchange into shares and subsequent resale can raise separate questions. Registered offerings, private-placement exemptions and offshore offerings need separate definitions when encountered, including Rule 144A and Regulation S where relevant.
7. **Section 3(a)(9):** an exchange-related exemption considered in the SEC correspondence we discussed. Its conditions require a separate lesson; “a bail-in happened” is not a substitute for examining those conditions.
8. **No-action letter:** a staff enforcement position on represented facts, not a blanket judicial ruling or proof of an actual bail-in.
9. Same issuer, subsidiary issuers and guarantees: why the identity of the old debtor and new share issuer can matter.
10. Direct conversion versus a two-stage exchange using temporary rights. Learn the UK example's term **PROPP** only when examining that process.
11. Notices, DTC and other depositories, record changes, trading suspensions, listing, transferability and delivery of shares: legal conversion must be operationally implemented.

**Cases already in our research, to revisit after the concepts:** HSBC's contractual AT1 trigger versus its senior debt's statutory bail-in treatment; RBC's contractual non-viability conversion formula versus senior statutory conversion; UBS's Swiss process and New York/Swiss-law debt; the Bank of England and UBS SEC staff positions discussed in the report. These are specific examples, not universal country templates.

**Ready to move on when:** You can explain why finding a US owner does not, by itself, tell us how a foreign bank would legally exchange the bond for shares.

## Tier 9 — How did we turn documents into evidence?

**Central idea:** The pilot linked a list of securities to a list of reported investments, then made a carefully limited calculation. Understanding the method does not require learning Python.

### The public-data path we actually used

1. **Define the inventory:** select HSBC Holdings plc securities from the bank's instrument disclosure. Distinguish nominal amounts from recognized amounts.
2. **Identify each security:** ISIN and CUSIP are identifiers, not investor categories. Names alone are unreliable. Identifier aliases and checks help prevent incorrect matches or duplicates.
3. **Collect holdings:** SEC EDGAR is a filing system; Form N-PORT reports investment-fund portfolios. It is not a census of every US investor. Money market funds are outside this particular reporting population.
4. **Align dates:** select June 30 holdings even when the filing was submitted in a later quarter. A filing archive's quarter is not automatically the holdings quarter.
5. **Choose one report per fund/date:** account for repeat filings and amendments. Registrant and fund-series identifiers help distinguish a filing organization from its individual funds.
6. **Join by security:** combine each reported holding with the corresponding security in the issuer inventory.
7. **Check units and scope:** shares, principal and market value are different fields. Preserve unresolved records separately instead of making an unsupported conversion.
8. **Calculate:** use compatible principal amounts and currency conversions, retaining securities without observed matches in the denominator.
9. **Validate:** inspect source PDF rows; check identifier validity, duplicates and implausible shares; compare bulk records with original filings; preserve securities-lending information.
10. **Preserve evidence:** source links, downloaded files, file fingerprints, selected records, exceptions and the calculation script make the result inspectable and reproducible. Reproducibility does not itself prove the source data are complete.

### What commercial data could add — discussed, not accessed

- **Bloomberg:** bond search and security data could help construct issuance populations. The Bank of Italy study used Bloomberg classifications and issuance data, checking amounts against Dealogic, Refinitiv and issuer announcements.
- **LSEG Workspace:** a documented `TR.IsTLACEligible` field and bond fields for identifiers, dates, amounts, currency, maturity and ranking offer a possible extraction route. Current eligibility is not automatically historical eligibility.
- **eMAXX:** a bond-holdings dataset available through LSEG offerings, with insurer, fund and pension information and principal amounts. Coverage and reporting vary; manager and owner records must be distinguished.
- **Lipper and Dealogic:** fund holdings and issuance data used together in the ESM research. The dataset's population determines what its percentage means.
- **ECB securities holdings statistics:** the Bank of Italy study's ownership source. Visibility of non-euro-area investors depends partly on reporting custodians.
- **Other routes:** insurer investment schedules such as NAIC Schedule D; pension reports; custodian records; Treasury cross-border securities statistics as possible context rather than an assumed TLAC-level census. Access and variable coverage would need verification before use.
- **Practical access:** a desktop subscription, an extra dataset, historical coverage, spreadsheet exports and API access are different entitlements. Public documentation verifies a capability, not the contents of our account or every security's coverage.
- **FSB reconstruction:** choosing issuers, classifying instruments, selecting issuance dates, reconciling amounts and converting currencies is a plausible way to build the quoted Bloomberg-based issuance figures. We did not recover the FSB's exact query or unpublished methodology.

**Ready to move on when:** You can narrate our pilot as “list the securities → find reported holdings → match → check → divide,” and identify what remains invisible.

## Tier 10 — Reassemble the actual findings without losing their meaning

**Central idea:** Each result is a sentence about a defined population, quantity and date. The qualification belongs to the finding, not in an optional footnote.

### The HSBC pilot

- We selected **129 parent-company securities** with **$161.957 billion of nominal principal** at June 30, 2025. Ordinary shares and the separately excluded instruments were outside this inventory.
- **449 observed funds** contributed **1,883 included positions** totaling approximately **$8.799 billion of principal**. Dividing by that inventory gives **5.43%**.
- The cohort consists of SEC-registered funds whose registrants reported US addresses and for which the exact-date reports were observed. It is not all US investors or verified ultimate US economic ownership.
- The categories were 14 AT1, 31 Tier 2 counting toward MREL, and 84 other eligible-liability securities. Their observed shares were approximately **2.73%, 4.39% and 6.24%**, respectively.
- The dollar-denominated subset gave **7.93%**; currency does not identify owner residence. The included holdings' **$8.825 billion market value** is a different measure from principal.
- For **US404280DC08**, $312.220 million of observed principal divided by $2 billion outstanding gives **15.61%**. Dodge & Cox Income Fund alone reported $199.080 million principal. These positions supplied a concrete original-filing validation and a possible vendor demonstration test.
- Sixteen matched positions had ambiguous units and were excluded from the principal calculation. Missing funds, other investor sectors, ultimate fund investors, later amendments and net hedging exposure remain gaps.
- Securities on loan were retained as reported investments, not added again. The checks against original Dodge & Cox and BlackRock filings tested extraction accuracy, not completeness of US ownership.

### Totals, issuance and classifications

- HSBC's reported regulatory TLAC was **$289.0 billion in June 2025** and **$299.1 billion in June 2026**. Neither is the same measure as our $161.957 billion parent-security inventory; the difference cannot simply be labelled ordinary equity.
- The June 2026 regulatory composition was **$127.7 billion CET1, $23.7 billion AT1, $28.3 billion Tier 2 and $119.4 billion other TLAC instruments**. Revisit why those categories differ from a simple debt/equity/convertible split.
- The FSB's reviewed population had estimated issuance of **$400 billion in 2016, $433 billion in 2017 and $360 billion in 2018**, with about **$144 billion through May 2019**. These are flows for that study's population, not a current global stock.
- The 2018 breakdown was **$36 billion AT1, $24 billion Tier 2, $52 billion non-preferred senior and $248 billion other senior debt**.
- The Bank of Italy study counted approximately **5,270 eligible securities globally during 2013–June 2020**, including **1,715 and €776 billion** for eight euro-area G-SIBs. A multi-year securities count is not a one-year deal count.
- Our inventory contained **18 securities originally issued in the first half of 2025, on nine dates, with $24.016 billion of June-date principal**. This is a surviving-inventory observation, not established full-period gross issuance proceeds.
- The SRB's **€119.789 billion gross MREL issuance in the second half of 2025**, and UBS's **$3.302 billion of new eligible senior debt in Q4 2025 despite a falling recognized balance**, illustrate different populations and the stock/flow distinction.

### Ownership studies and remaining questions

- The Bank of Italy study's **9.4% US share of visible holdings** and the ESM's **12% US share of sampled fund holdings** do not form a global 9–12% ownership range.
- The ESM's **€221 billion** fund-holdings total implies approximately **€26.52 billion** for its US category. That is not US holdings divided by all outstanding global TLAC.
- We have not established total global TLAC securities value, total US ownership, or a complete US insurer/pension breakdown.
- The 2026 UK and UBS SEC correspondence and our contract examples address execution arrangements, not evidence of completed bail-ins.
- The strongest next commercial-data test would compare the existing HSBC inventory and known fund position with vendor records, then determine what additional dated insurer and pension positions are actually available. This remains a future investigation, paused during learning.

**Ready to finish the detour when:** You can explain the 5.43% result to a new reader, say why the other percentages do not contradict it, and specify the missing data needed for the broader question.

## Where each original research question belongs

| Original question | Concepts needed | Final learning destination |
|---|---|---|
| 1. What percentage is owned by US investors? | Ownership chains, country definitions, units, missing coverage | Tiers 6–7, then the limits in Tier 10 |
| 2. What percentage is owned by a US entity type? | Funds versus managers, insurers, pensions, direct versus pooled holdings | Tiers 6–7 and sector data in Tier 9 |
| 3. What percentage of one G-SIB's instruments is US-owned? | Legal issuer, security inventory, holdings match | The HSBC calculation in Tiers 9–10 |
| 4. What is the global total value? | Regulatory resources versus principal versus market value; group boundaries | Tiers 3, 5 and 7 |
| 5. What is one bank's total value? | Same distinctions plus consolidation | HSBC reconciliation in Tier 10 |
| 6. What is annual issuance value? | Flows, currency, dates, additions, eligibility | Tiers 2, 5, 7 and 9 |
| 7. How many issuances occurred? | Transactions versus series/tranches/identifiers; surviving inventories | Tiers 2 and 7 |
| 8. What is the instrument breakdown? | Legal form versus accounting and regulatory categories | Tier 5, applied in Tier 10 |
| 9. How do contracts differ? | Ranking, triggers, conversion and write-down | Tiers 4–5 and 8 |
| 10. How do foreign banks, US law and bail-in interact? | Legal entities, cross-border claims and securities exchanges | Tier 8 |
| How would Bloomberg/LSEG help, and what did the FSB do? | Classification, data sources, historical coverage and reproducibility | Tier 9 |

## Reading and teaching sequence

Start with Tier 1, not the findings table. At each step, use a short explanation, a worked example if needed, and one question. Advance when the concept is usable; revisit earlier ideas in later examples. The checks in this document are a map for the tutor, not a batch of questions for the learner to answer at once.

For the first conversation, use only the bakery's lender and owner. We will add its balance sheet, then a tradeable bond, then the bank and its losses. There is no need to memorize TLAC acronyms now.

## References already in the conversation

These links preserve the research trail. They are not prerequisite reading.

- [Full findings, original documents, caveats and pilot method](RESULTS.md).
- [Reproducible HSBC calculation](run_hsbc_pilot.py).
- [FSB 2019 TLAC implementation review](https://www.fsb.org/uploads/P020719.pdf): issuance estimates and the surrounding framework.
- [Bank of Italy, *TLAC-eligible debt: who holds it?*](https://www.bancaditalia.it/pubblicazioni/qef/2021-0604/QEF_604_21.pdf): commercial issuance data linked to custody-based holdings.
- [ESM, *The hidden web*](https://www.esm.europa.eu/blog/hidden-web-how-loss-absorbing-bonds-connect-banks-and-investment-funds): fund holdings of MREL bonds.
- [LSEG TLAC field example](https://community.developers.lseg.com/discussion/comment/90672/) and [bond-data extraction example](https://developers.lseg.com/en/article-catalog/article/debt-structure-analysis-on-an-organizational-level).
- [LSEG eMAXX description](https://www.lseg.com/en/data-analytics/products/workspace/updates/emaxx) and [detailed data brochure](https://www.lseg.com/content/dam/data-analytics/en_us/documents/brochures/lseg-data-for-quant-research-brochure.pdf).
- [*Granular Treasury Demand with Arbitrageurs*](https://www.frbsf.org/wp-content/uploads/W-Li-paper.pdf): its data appendix illustrates differences in insurer and pension coverage; it is not a TLAC ownership estimate.
- [Bloomberg reference data](https://professional.bloomberg.com/products/data/enterprise-catalog/reference/) and [bond-search example](https://assets.bbhub.io/professional/sites/12/983415_Bloomberg_IntroductionToAIFunctions.pdf).

## Learning progress

- Research investigation: paused for this detour.
- Learning map: prepared.
- Starting lesson: Tier 1, lender versus owner.
- Understanding checks: not yet completed; no prior knowledge is assumed from the sophistication of the research questions.
