import requests
import json

# Test the step-by-step breakdown API with multi-line discount
test_data = {
    'carrier': 'STATEFARM',
    'state': 'CA',
    'zip_code': '95123',
    'vehicle': {
        'year': 2017,
        'make': 'PORSCHE',
        'model': '718 CAYMAN',
        'series': '',
        'package': 'N/A',
        'style': '2D CPE',
        'engine': 'N/A',
        'msrp': None
    },
    'coverages': {
        'BIPD': {'selected': True, 'limits': '50/100', 'deductible': None},
        'COLL': {'selected': True, 'limits': None, 'deductible': 1000},
        'COMP': {'selected': True, 'limits': None, 'deductible': 1000}
    },
    'drivers': [{
        'driver_id': '1',
        'years_licensed': 21,
        'marital_status': 'M',
        'age': 37
    }],
    'discounts': {
        'good_driver': True,
        'loyalty_years': 3,
        'multi_line': 'Condo, Homeowners, or Farm/Ranch Policy'
    },
    'special_factors': {
        'federal_employee': False,
        'transportation_network_company': False,
        'transportation_of_friends': False
    },
    'usage': {
        'annual_mileage': 10000,
        'type': 'Pleasure / Work / School',
        'single_automobile': True
    }
}

try:
    response = requests.post('http://127.0.0.1:8000/step-by-step-breakdown/', json=test_data)
    print(f'Status: {response.status_code}')
    
    if response.status_code == 200:
        data = response.json()
        print('\nMulti-line discount factors in response:')
        for coverage in ['BIPD', 'COLL', 'COMP']:
            multi_line_factor = data.get('breakdowns', {}).get('discount_factors', {}).get(coverage, {}).get('multi_line', 'NOT FOUND')
            print(f'{coverage}: {multi_line_factor}')
        
        print('\nFull discount_factors structure:')
        print(json.dumps(data.get('breakdowns', {}).get('discount_factors', {}), indent=2))
    else:
        print(f'API Error: {response.status_code} - {response.text}')
        
except Exception as e:
    print(f'Connection Error: {e}')
    print('Make sure the PY Pricing Service is running on port 8000')
