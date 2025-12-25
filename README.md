# Toronto Shelter Accessibility Optimization

This repository contains a data-driven modeling framework for analyzing and improving accessibility within Toronto’s homeless shelter system. The project combines **socioeconomic hotspot identification**, **behavioral simulations**, and a **two-stage mathematical optimization model** to study how shelter capacity, geography, and eligibility constraints affect travel distance and unmet demand across the city.

## Problem Motivation

Toronto’s shelter system operates near full saturation (≈98–99% occupancy), while homelessness demand is unevenly distributed across the city. Individuals often travel long distances between shelters with no guarantee of placement, leading to inefficiencies and inequitable access, particularly for women and for residents of peripheral neighborhoods.

This project addresses the core question:

> **How can shelter spaces be allocated to minimize travel distance while respecting capacity limits, gender eligibility, and geographic imbalance, without reducing the number of people served?**

## Data Sources

All data are publicly available and sourced from the **City of Toronto Open Data Portal** and **Statistics Canada**:

- **Shelter Occupancy & Capacity (2017–2025)**  
  Nightly occupancy, bed counts, utilization rates
- **Monthly Shelter System Flows (2018–2025)**  
  Aggregate inflow/outflow of individuals in the shelter system
- **2021 Census FSA Profiles**  
  Socioeconomic indicators used to estimate homelessness vulnerability

The analysis is restricted to **men’s and women’s shelters within Toronto** due to data consistency and reporting quality.

## Methodology Overview

### 1. Hotspot Identification
- Constructed a **composite poverty index** using:
  - Median after-tax income  
  - LIM-AT population count  
  - LIM-AT prevalence
- Normalized indicators and selected the **top 16 FSAs** as homelessness demand hotspots.
- Hotspot centroids serve as origins in simulations and optimization.

### 2. Distance Matrix
- Computed Euclidean distances (km) between hotspot centroids and shelter locations.
- Coordinates projected to EPSG:3857 for metric accuracy.

### 3. Behavioral Simulations
Two heuristic benchmarks approximate how individuals may search for shelters:

- **Random Allocation**  
  Individuals attempt randomly chosen eligible shelters until capacity is exhausted.
- **Greedy Allocation**  
  Individuals attempt the nearest eligible shelter first, then proceed outward.

Simulations provide a realistic lower bound for performance and upper bounds for travel distance outcomes.

### 4. Two-Stage Optimization Model
Implemented in **Gurobi**, the optimization prioritizes service coverage before efficiency:

**Stage 1 – Minimize Unsheltered Population**
- Maximizes the number of individuals placed under capacity and eligibility constraints.

**Stage 2 – Minimize Travel Distance**
- Fixes the unsheltered population at the Stage 1 optimum.
- Reallocates assignments to minimize total travel distance without reducing service.

This avoids the pathological solution of “minimizing distance by leaving people unserved.”

## Key Findings

- The shelter system is **fully capacity-constrained year-round**; algorithmic strategy does not reduce total unsheltered counts.
- Optimization **substantially reduces travel distance** relative to random and greedy behavior.
- Persistent **gender inequity**: women travel ~40% farther on average even under optimal allocation.
- Severe geographic service deserts exist, especially in **North York (M2N)** and other peripheral FSAs.
- Downtown demand is fragmented across many shelters, indicating a lack of consolidated intake capacity.

## Policy-Relevant Recommendations

- Add high-capacity, mixed-gender shelter infrastructure in **M2N**.
- Address gender imbalance in **M5A** via women-specific capacity.
- Establish a consolidated intake “hub” in **M5B** to reduce assignment fragmentation.
- Deploy **seasonal surge shelters** in peripheral hotspots during winter months.

## Repository Structure
```
toronto-shelter-optimization-framework/
│
├── data/
│   ├── cache/
│   ├── processed/
│   └── raw/
│
├── notebooks/
│   ├── 1.0-initial-exploratory-data-analysis.ipynb
│   ├── 2.0-deep-exploratory-data-analysis.ipynb
│   ├── 3.0-initial-hotspot-computation.ipynb
│   ├── 4.0-further-data-processing.ipynb
│   ├── 5.0-multi-hotspot-and-distance-calculations.ipynb
│   └── 6.0-simulations.ipynb
│
├── reports/
│   └── report.pdf # Documentation and explanation of the entire project
│
├── src/
│   ├── etl/
│   │   ├── extract.py
│   │   ├── load.py
│   │   └── transform.py
│   │
│   ├── optimization/
│   │   ├── model.py
│   │   └── run.py
│   │
│   ├── config.py
│   └── run_etl.py
│
├── .gitignore
├── pyproject.toml
└── README.md
```
