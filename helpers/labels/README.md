# README for Service Public Labels

## Overview

This directory contains JSON files that define the whitelist and blacklist of entities classified as public services within our API. This classification is crucial for determining access rights and service eligibility.

## Current API Status

- **RATP**: it is currently classified as a public service based on the `nature juridique` field; however, it should not be classified as such. Therefore, it is included in the blacklist.

- **URSSAF Regions**: The URSSAF entities at the regional level are not currently classified as public services according to their `nature juridique`, but they should be. As a result, they are included in the whitelist.

- **BPI France**: Similar to the URSSAF regions, BPI France should also be classified as a public service and is therefore included in the whitelist.

## Legal Authorization

The legal department has authorized changes to the current classification. This will enable us to:
- Reclassify the RATP as a non-public service.
- Grant access to agents for members of the URSSAF, thereby enhancing service delivery.


### Sources of SIREN Numbers:
The list of URSSAF entities is obtained from [this official source](https://fichierdirect.declaration.urssaf.fr/TablesReference.htm). The SIREN numbers are extracted from the SIRET numbers provided in that list.
