# Selected G-SIB organization PermIDs

Retrieved on **September 22, 2026** from LSEG's public [Open PermID site](https://permid.org/), using its organization search in Chrome without logging in. These identifiers did not require Workspace or eMAXX access.

## Population

The [FSB November 2025 list](https://www.fsb.org/uploads/P271125.pdf) contains 29 G-SIBs. Excluding the eight US-based and five China-based groups leaves the **16 groups below**. The exclusion applies to the home country of the banking group, not the location of each subsidiary or the currency of its bonds. HSBC and Standard Chartered are included as UK groups. This is a current-list population, not a reconstruction of historical G-SIB membership.

## Organization mapping

The organization name is transcribed from LSEG's public record. The numeric column contains an **organization PermID**, not an equity instrument, quotation or bond identifier. The `1-` in the source URL is not part of the numeric value shown here.

| Selected G-SIB group | Home country | LSEG organization name | Organization PermID | Public source |
|---|---|---|---|---|
| Royal Bank of Canada | Canada | Royal Bank of Canada | 8589934213 | [Record](https://permid.org/1-8589934213) |
| Toronto-Dominion | Canada | The Toronto-Dominion Bank | 4295862902 | [Record](https://permid.org/1-4295862902) |
| BNP Paribas | France | BNP Paribas SA | 8589934326 | [Record](https://permid.org/1-8589934326) |
| Groupe BPCE | France | Bpce SA | 5000084509 | [Record](https://permid.org/1-5000084509) |
| Groupe Crédit Agricole | France | Credit Agricole SA | 8589934312 | [Record](https://permid.org/1-8589934312) |
| Société Générale | France | Societe Generale SA | 5000039357 | [Record](https://permid.org/1-5000039357) |
| Deutsche Bank | Germany | Deutsche Bank AG | 4295869482 | [Record](https://permid.org/1-4295869482) |
| Mitsubishi UFJ Financial Group | Japan | Mitsubishi UFJ Financial Group, Inc. | 5000000933 | [Record](https://permid.org/1-5000000933) |
| Mizuho Financial Group | Japan | Mizuho Financial Group, Inc. | 4295878505 | [Record](https://permid.org/1-4295878505) |
| Sumitomo Mitsui Financial Group | Japan | Sumitomo Mitsui Financial Group, Inc. | 4295878766 | [Record](https://permid.org/1-4295878766) |
| ING | Netherlands | ING Groep N.V. | 4295884647 | [Record](https://permid.org/1-4295884647) |
| Santander | Spain | Banco Santander S.A. | 8589934205 | [Record](https://permid.org/1-8589934205) |
| UBS | Switzerland | UBS Group AG | 5043337560 | [Record](https://permid.org/1-5043337560) |
| Barclays | United Kingdom | Barclays PLC | 8589934333 | [Record](https://permid.org/1-8589934333) |
| HSBC | United Kingdom | HSBC Holdings PLC | 8589934275 | [Record](https://permid.org/1-8589934275) |
| Standard Chartered | United Kingdom | Standard Chartered PLC | 4295895205 | [Record](https://permid.org/1-4295895205) |

## Verification and limits

All 16 names and identifiers were observed in the Organization section of the public search results or their organization detail pages. For 15 records, the detail page also confirmed the expected domicile, active status and a legal entity identifier (LEI). Deutsche Bank's detail page remained on its loading screen; its name and PermID were verified in the [public organization search](https://permid.org/entity-search;search=Deutsche%20Bank), which also linked to the bank's official website. Its German home-country classification was not obtained from that stalled detail page.

These are verified entity identifiers and starting points for bond discovery. They have **not yet been tested as group search filters in Workspace**, and do not themselves establish that a query includes every entity belonging to the corresponding G-SIB.

Two mappings particularly need a group-coverage check:

- **Groupe Crédit Agricole → Credit Agricole SA:** Crédit Agricole SA is the group's central body and a major issuing entity. The full group also includes regional banks, which own a majority stake in Crédit Agricole SA. A search that follows only subsidiaries of SA could therefore be narrower than the G-SIB group. Keep this ID as a starting point and inspect the issuing entities returned before claiming complete group coverage. [Official group structure](https://www.credit-agricole.com/en/group/group-structure).
- **Groupe BPCE → Bpce SA:** BPCE is the group's central body. Its cooperative banking networks also need to be considered when evaluating the coverage of a parent-based search. [Official organization structure](https://www.groupebpce.com/en/the-group/organization/).

For the other groups, the selected record is the parent holding company or principal bank corresponding to the G-SIB name. For example, this file selects UBS Group AG rather than UBS AG, Barclays PLC rather than Barclays Bank PLC, and ING Groep rather than an operating-bank subsidiary.

## Next use in Workspace

Use these organization IDs to discover associated bonds, retaining both the selected group and each bond's actual issuer. Then retrieve `TR.IsTLACEligible` for the bond identifiers and preserve Y, N and missing results with the extraction date.

LSEG's [worked debt-structure example](https://developers.lseg.com/en/article-catalog/article/debt-structure-analysis-on-an-organizational-level) demonstrates a search using `ParentOAPermID` and uses Santander's `8589934205`, independently matching the public record collected here. Confirm the parent relationship used by that search for each selected group; do not assume organization identity proves a complete regulatory group boundary.

### Copyable organization IDs

Same order as the table:

```text
8589934213
4295862902
8589934326
5000084509
8589934312
5000039357
4295869482
5000000933
4295878505
4295878766
4295884647
8589934205
5043337560
8589934333
8589934275
4295895205
```
