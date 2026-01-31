# KRW Ecological Status Classifications in AQUO

## ClassificatieKRWbiologischOW (KRW Biological Classification for Surface Water)
This contains the 5-level biological ecological status classification required by the EU Water Framework Directive:

| ID | Classification | Description | Valid from |
|----|----------------|-------------|------------|
| 5  | Zeer goed      | Very good   | 2011-10-06 |
| 1  | Goed           | Good        | 2011-10-06 |
| 2  | Matig          | Moderate    | 2011-10-06 |
| 3  | Ontoereikend   | Poor        | 2011-10-06 |
| 4  | Slecht         | Bad         | 2011-10-06 |

**Key Finding**: This directly implements the EU Water Framework Directive's ecological status classification system:
- **Zeer goed** = "Very Good" = No or only very minor anthropogenic changes
- **Goed** = "Good" = Slight anthropogenic influence
- **Matig** = "Moderate" = Moderate anthropogenic influence  
- **Ontoereikend** = "Poor" = Major anthropogenic changes
- **Slecht** = "Bad" = Severe anthropogenic changes

## ClassificatieKRWchemischOW (KRW Chemical Classification for Surface Water)
Chemical status has only 2 levels as required by the WFD:

| ID | Classification | Description | Valid from |
|----|----------------|-------------|------------|
| 1  | Voldoet        | Compliant   | 2011-10-06 |
| 2  | Voldoet niet   | Not compliant | 2011-10-06 |

## ClassificatieKRWGW (KRW Classification for Groundwater)
Groundwater has simplified 2-level classification:

| ID | Classification | Description | Valid from |
|----|----------------|-------------|------------|
| 1  | Goed           | Good        | 2011-10-06 |
| 2  | Ontoereikend   | Poor        | 2011-10-06 |

## Analysis

These AQUO classifications are the Dutch implementation of the EU Water Framework Directive's status assessment framework. The biological classification directly maps to Article 2(22) and Annex V of the WFD.

**Critical insight**: The Dutch implementation preserves the EU directive's anti-anthropogenic hierarchy where "Zeer goed" (Very good) explicitly requires "no or only very minor anthropogenic changes" - confirming our analysis of the directive's ideological framework.

## Usage in AQUO System

The KRW classification codes are used by **Parameter** domains in the AQUO system:

### Key Parameters Using Classifications:

| Parameter Code | Description | Purpose |
|----------------|-------------|---------|
| ECOLT | Ecologie toestand of potentieel | Overall ecological status assessment |
| CHEMT | Chemische toestand | Chemical status assessment |
| GWKWALT | Toestand grondwaterkwaliteit | Groundwater quality status |
| GWKWANT | Toestand grondwaterkwantiteit | Groundwater quantity status |
| VIS | Vis-kwaliteit | Fish quality indicator |
| MAFYTEN | Macrofyten-kwaliteit | Macrophyte quality indicator |
| MAALGEN | Macroalgen-kwaliteit | Macroalgae quality indicator |

### Implementation Pattern:
- **Individual measurements** are stored in WKP data (physical/chemical/biological parameters)
- **Status assessments** use these Parameter codes to reference KRW classification values
- **Classifications** are applied at water body level using these standardized assessment parameters
- **Reporting** to EU uses these aggregated status classifications per water body

**Location in data**: The classification values ("Zeer goed", "Goed", "Matig", "Ontoereikend", "Slecht") are referenced by assessment parameters in the AQUO Parameter domain, which are then used in water body status reporting rather than individual measurements.