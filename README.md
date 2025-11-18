# Tech Jobs Market Analysis 2024-25

An end-to-end data analytics project analyzing the U.S. tech job market using 100K+ job listings. This project combines data from two major datasets to provide comprehensive insights into job trends, salary patterns, and market demand across different locations and job types.

## 📊 Project Overview

This project analyzes tech job market trends in the United States by performing comprehensive data cleaning, wrangling, exploratory data analysis (EDA), and visualization using Tableau. The analysis aims to answer critical business questions about the tech job market and test hypotheses regarding salary distributions, location preferences, and job type trends.

## 🎯 Business Objectives

- Identify salary trends across different tech job roles and locations
- Analyze the distribution of remote vs. on-site opportunities
- Understand geographic concentration of tech jobs
- Examine job type preferences (full-time, contract, part-time, etc.)
- Provide actionable insights for job seekers and employers

## 📊 Visit the final Tableau dashboard here: [Tech Jobs Market Analysis Dashboard](https://public.tableau.com/views/Indeed_com-USTechJobs2024Overview/PublicPresentation?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)

## 📁 Datasets

This project utilizes one comprehensive Kaggle dataset:

1. **[100K US Tech Jobs - Winter 2024](https://www.kaggle.com/datasets/christopherkverne/100k-us-tech-jobs-winter-2024?resource=download)**
   - Contains extensive job listings from the U.S. tech sector
   - Includes salary information, locations, and job types

## 🔧 Project Workflow

### 1. Data Cleaning & Wrangling

The data cleaning process (`data-cleaning.ipynb`) includes:

- **Missing Value Handling**
  - Replaced null company names with "Unknown"
  - Imputed missing salary values (min/max) with median values
  - Filled missing mean salary with the overall mean

- **Location Data Parsing**
  - Developed sophisticated location parser to extract city, state, and country
  - Normalized location formats (e.g., "US", "USA", "United States" → "US")
  - Created state abbreviation mapping for all 50 U.S. states
  - Detected and flagged remote positions
  - Generated user-friendly location display format

- **Job Type Standardization**
  - Parsed multiple job type formats (e.g., "full-time", "fulltime", "FTE")
  - Implemented priority-based job type selection (fulltime > contract > parttime > internship)
  - Created categorical display column for visualization

- **Salary Field Engineering**
  - Calculated min, max, and mean salary fields
  - Handled edge cases and outliers
  - Ensured data consistency across salary columns

### 2. Exploratory Data Analysis (EDA)

Key analytical components:

- Statistical summaries of salary distributions
- Location-based job market analysis
- Remote vs. on-site job distribution
- Job type frequency analysis
- Cross-sectional analysis of multiple variables

### 3. Data Visualization (Tableau)

Created interactive Tableau dashboards to:

- Visualize salary ranges across different states and cities
- Compare remote vs. on-site job opportunities
- Analyze job type distribution
- Display geographic heat maps of job concentrations
- Test hypotheses through visual analytics

## 📈 Key Features

- **Robust Data Pipeline**: Automated cleaning and transformation of 100K+ records
- **Smart Location Parser**: Handles various location formats with country/state normalization
- **Salary Analysis**: Comprehensive salary metrics (min, max, mean) for market insights
- **Remote Work Insights**: Clear identification of remote opportunities
- **Scalable Architecture**: Code designed for reusability and extension

## 🛠️ Technologies Used

- **Python 3.x**
  - pandas: Data manipulation and analysis
  - numpy: Numerical operations
  - regex: Text parsing and pattern matching
- **Jupyter Notebook**: Interactive development and documentation
- **Tableau**: Data visualization and dashboard creation
- **Excel**: Initial data format and exploration

## 📂 Project Structure

```
data_analytics_end_to_end_project/
├── data-cleaning.ipynb          # Main notebook with data cleaning and wrangling
├── README.md                     # Project documentation
```

## 🚀 Getting Started

### Prerequisites

```bash
pip install pandas numpy openpyxl
```

### Running the Analysis

1. Download the datasets from the Kaggle links provided above
2. Place the raw data file (`all_jobs.xlsx`) in the project root directory
3. Open and run `data-cleaning.ipynb` in Jupyter Notebook
4. The cleaned dataset will be saved to `output/cleaned_all_jobs.csv`
5. Import the cleaned CSV into Tableau for visualization

## 📊 Output Schema

The cleaned dataset includes the following columns:

| Column | Description |
|--------|-------------|
| `title` | Job title |
| `company` | Company name (or "Unknown") |
| `location_city` | Parsed city name |
| `location_state` | Standardized state abbreviation |
| `location_country` | Normalized country code |
| `job_type_display` | Primary job type (Full-time, Contract, etc.) |
| `is_remote_display` | Remote or On-site classification |
| `min_salary` | Minimum salary offered |
| `max_salary` | Maximum salary offered |
| `mean_salary` | Average salary |

## 🔍 Key Insights

The analysis reveals:

- Comprehensive view of the tech job market landscape
- Salary benchmarks across different locations and job types
- Remote work trends in the tech industry
- Geographic distribution of opportunities
- Market demand patterns

## 📝 Methodology

1. **Data Collection**: Aggregated data from two complementary Kaggle datasets
2. **Data Cleaning**: Implemented robust parsing and normalization algorithms
3. **Data Transformation**: Created derived features for enhanced analysis
4. **Analysis**: Conducted statistical and exploratory data analysis
5. **Visualization**: Developed interactive Tableau dashboards
6. **Hypothesis Testing**: Validated business questions through visual analytics

## 👤 Author

**Bruno Pulheze**
- GitHub: [@brunopulheze](https://github.com/brunopulheze)

## 📅 Project Timeline

- **Data Collection**: Winter 2024-25
- **Analysis Period**: 2024-25 Tech Job Market
- **Last Updated**: November 2025

## 🤝 Contributing

This is an educational project. Feel free to fork and adapt for your own analysis!

## 📄 License

This project uses publicly available datasets from Kaggle. Please refer to the original dataset licenses for usage terms.

## 🙏 Acknowledgments

- Kaggle community for providing high-quality datasets
- Christopher Kverne for the 100K US Tech Jobs dataset
- Umar Rehan Khan for the Tech Job Listings with Skills dataset

---

*This project was completed as part of the Ironhack Data Science & AI Bootcamp - Week 23 Assignment*
