import sys
import os

# Add current directory to path
sys.path.append(os.getcwd())

from app.services.vector_databases.vehicle_rates_search import initialize_vehicle_rates_db, get_vehicle_rates_db, VehicleRatesVectorDB
from app.services.vehicle_search.vehicle_spec_orchestrator import VehicleSpecOrchestrator

def test_singleton():
    print("Testing Singleton Implementation...")
    
    # 1. Test Initialization
    print("Initializing DB...")
    initialize_vehicle_rates_db()
    
    # 2. Get Instance
    db1 = get_vehicle_rates_db()
    assert db1 is not None, "Database instance should not be None"
    assert isinstance(db1, VehicleRatesVectorDB), "Instance should be of type VehicleRatesVectorDB"
    print("First instance retrieved successfully.")
    
    # 3. Test Re-initialization (should be ignored)
    print("Attempting re-initialization...")
    initialize_vehicle_rates_db()
    db2 = get_vehicle_rates_db()
    
    assert db1 is db2, "Instance should be the same after re-initialization attempt"
    print("Singleton pattern verified: Instances are identical.")
    
    # 4. Test Orchestrator Usage
    print("Testing VehicleSpecOrchestrator integration...")
    orchestrator = VehicleSpecOrchestrator()
    assert orchestrator.vehicle_rates_vector_db is db1, "Orchestrator should use the singleton instance"
    print("Orchestrator integration verified.")
    
    print("\nALL TESTS PASSED")

if __name__ == "__main__":
    try:
        test_singleton()
    except Exception as e:
        print(f"\nTEST FAILED: {e}")
        sys.exit(1)
