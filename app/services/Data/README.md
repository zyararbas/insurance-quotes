# California State Farm Data Directory

This directory contains lookup data and pricing tables for the California State Farm pricing service.

## Directory Structure

```
app/data/
├── lookup_tables/          # Rate tables and lookup data
├── pricing_data/           # Pricing algorithms and factors
├── carrier_data/           # State Farm specific data
└── california_data/        # California state-specific data
```

## Data Files to Add

When you copy your data from the other project, place the files in the appropriate subdirectories:

- **Rate Tables**: Place in `lookup_tables/`
- **Pricing Algorithms**: Place in `pricing_data/`
- **State Farm Specific Data**: Place in `carrier_data/`
- **California Regulations**: Place in `california_data/`

## File Formats

The service expects data in JSON format for easy loading. If your data is in other formats (CSV, Excel, etc.), you may need to convert it or update the data loading logic in `california_statefarm_service.py`.
