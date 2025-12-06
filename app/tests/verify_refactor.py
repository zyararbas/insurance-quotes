import sys
import os
from unittest.mock import MagicMock, patch

# Add the project root to the python path
sys.path.append(os.getcwd())

from app.routes.insurance_quotes import _calculate_single_rating, create_quote
from app.models.models import RatingInput

def test_calculate_single_rating():
    print("Testing _calculate_single_rating...")
    
    # Mock RatingInput
    rating_input = MagicMock(spec=RatingInput)
    rating_input.carrier = "aaa"
    
    # Mock PricingOrchestrator
    with patch("app.routes.insurance_quotes.PricingOrchestrator") as MockOrchestrator:
        instance = MockOrchestrator.return_value
        instance.calculate_premium.return_value = {"premium": 100}
        
        result = _calculate_single_rating(rating_input)
        
        assert result == {"premium": 100}
        print("  _calculate_single_rating passed.")

def test_create_quote():
    print("Testing create_quote...")
    
    # Mock AdapterService
    with patch("app.routes.insurance_quotes.adapter_service") as mock_adapter:
        # Create dummy rating inputs
        input1 = MagicMock(spec=RatingInput)
        input1.carrier = "aaa"
        input2 = MagicMock(spec=RatingInput)
        input2.carrier = "farmers"
        
        mock_adapter.create_rating_inputs_from_payload.return_value = [input1, input2]
        
        # Mock PricingOrchestrator
        with patch("app.routes.insurance_quotes.PricingOrchestrator") as MockOrchestrator:
            instance = MockOrchestrator.return_value
            instance.calculate_premium.side_effect = [{"premium": 100}, {"premium": 200}]
            
            # Call create_quote (it's async, but we can run it if we mock everything right, 
            # or just call the logic if we didn't make it async dependent. 
            # Wait, create_quote is async def. We need to run it with asyncio or just check logic.)
            # Since we are just testing the logic flow and we mocked the dependencies, 
            # we can run it with asyncio.run() or just inspect the code.
            # But wait, create_quote calls adapter_service synchronously and _calculate_single_rating synchronously.
            # So we can just await it.
            
            import asyncio
            payload = {"some": "data"}
            results = asyncio.run(create_quote(payload))
            
            assert len(results) == 2
            assert results[0] == {"premium": 100}
            assert results[1] == {"premium": 200}
            print("  create_quote passed.")

if __name__ == "__main__":
    try:
        test_calculate_single_rating()
        test_create_quote()
        print("All tests passed!")
    except Exception as e:
        print(f"Test failed: {e}")
        sys.exit(1)
