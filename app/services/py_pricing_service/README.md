# Insurance Quotes - Pricing Engine

A comprehensive insurance pricing engine for State Farm California auto insurance, featuring microservices architecture and comprehensive rating data.

## Project Structure

```
insurance-quotes/
├── data/                           # Insurance rating tables and data
│   └── STATEFARM_CA_Insurance__tables/
│       ├── base_factors/           # Core insurance rates and territory factors
│       ├── car_factors/            # Vehicle-related rating factors
│       ├── coverage_factors/       # Coverage limits and deductible factors
│       ├── discounts/              # Various discount factors
│       └── driver_factors/         # Driver-related rating factors
└── py-pricing-service/             # Python microservices pricing engine
    ├── main.py                     # FastAPI application entry point
    ├── models/                     # Data models and schemas
    ├── services/                   # Microservices for pricing calculations
    │   ├── lookup_services/        # Individual factor lookup services
    │   └── aggregation_services/   # Service aggregation and orchestration
    ├── utils/                      # Data loading and transformation utilities
    └── requirements.txt            # Python dependencies
```

## Features

- **Microservices Architecture**: Modular, scalable pricing services
- **Comprehensive Data**: State Farm CA insurance rating tables
- **Multi-Coverage Support**: BIPD, COLL, COMP, MPC, and U coverage calculations
- **Driver Factor Calculations**: Advanced driver adjustment factors with safety record scoring
- **Vehicle Rating Lookups**: Progressive vehicle selection with rating group factors
- **Real-time API**: FastAPI-based REST endpoints for pricing calculations

## Quick Start

### Prerequisites

- Python 3.8+
- pip

### Installation

1. **Navigate to the pricing service directory:**
   ```bash
   cd py-pricing-service
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Start the API server:**
   ```bash
   python main.py
   ```

4. **Access the API:**
   - API Documentation: `http://localhost:8000/docs`
   - Health Check: `http://localhost:8000/health`

## API Endpoints

### Pricing Calculation
- `POST /calculate-premium` - Calculate comprehensive insurance premium
- `POST /calculate-driver-factors` - Calculate driver adjustment factors
- `POST /lookup-vehicle-rating` - Lookup vehicle rating groups

### Data Lookup Services
- `GET /territory-factor/{zip_code}` - Get territory factor by ZIP code
- `GET /base-rate/{coverage}` - Get base rate for coverage type
- `GET /vehicle-rating/{make}/{model}/{year}` - Get vehicle rating groups

## Data Sources

The pricing engine uses comprehensive State Farm California rating tables including:

- **Base rates** and **territory factors** by ZIP code
- **Vehicle rating groups** and **model year factors**
- **Coverage limits** and **deductible factors**
- **Driver factors** and **safety record scoring**
- **Discount tables** for various eligibility criteria

## Architecture

The system uses a microservices architecture with:

- **Individual Lookup Services**: Handle specific factor lookups
- **Aggregation Services**: Combine multiple factors for comprehensive calculations
- **Data Loader**: Efficiently loads and caches rating table data
- **FastAPI Framework**: High-performance async API endpoints

## Development

### Adding New Coverage Types

1. Create new lookup services in `services/lookup_services/`
2. Implement aggregation logic in `services/aggregation_services/`
3. Add API endpoints in `main.py`
4. Update data models as needed

### Testing

Run the test suite:
```bash
cd py-pricing-service
python test_microservices.py
python test_full_calculation.py
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the MIT License.
