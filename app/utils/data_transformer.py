from app.models.graph_models import InsuranceGraphInput
from app.models.models import RatingInput, Vehicle, Coverages, Coverage, Driver, Discounts, Usage, SpecialFactors
import re

def transform_graph_to_rating_input(graph_input: InsuranceGraphInput) -> RatingInput:
    """
    Transforms the insurance graph data into the RatingInput format
    that the pricing engine expects.
    """
    graph_data = graph_input.data
    policy = graph_data.policy
    
    # --- Extract and Map Data ---
    
    # 1. Extract Zip Code from address
    zip_code_match = re.search(r'(\d{5})(-\d{4})?$', policy.mainAddress)
    zip_code = zip_code_match.group(1) if zip_code_match else "90210" # Default zip

    # 2. Map Vehicle (using the first vehicle for now)
    # The current pricing engine supports one vehicle per rating request.
    first_vehicle_graph = graph_data.vehicles[0]
    vehicle = Vehicle(
        year=int(first_vehicle_graph.modelYear),
        make=first_vehicle_graph.vehicleMake,
        model="UNKNOWN", # Graph data does not provide model, which is required.
    )

    # 3. Map Drivers
    # Graph data is sparse. We must use defaults for required fields.
    drivers = []
    for i, d in enumerate(graph_data.drivers):
        drivers.append(Driver(
            driver_id=f"driver_{i+1}_{d.givenName}",
            years_licensed=10, # Default value
            age=30, # Default value
            marital_status="M", # Default value
        ))

    # 4. Map Coverages
    # This is complex due to the separate coverage and vehicleCoverage lists.
    
    # Policy-level coverages (liability)
    bi_limit = "0"
    pd_limit = "0"
    mpc_limit = None
    um_limit_person = "0"
    um_limit_occurrence = "0"

    for cov in graph_data.coverages:
        if cov.coverageName == "Bodily Injury":
            bi_limit = cov.limitPerPerson.replace(',', '') if cov.limitPerPerson else "0"
        if cov.coverageName == "Property Damage":
            pd_limit = cov.limitPerOccurrence.replace(',', '') if cov.limitPerOccurrence else "0"
        if cov.coverageName == "Medical Payments":
            mpc_limit = cov.limitPerPerson.replace(',', '') if cov.limitPerPerson else None
        if cov.coverageName == "Uninsured Motorists":
            um_limit_person = cov.limitPerPerson.replace(',', '') if cov.limitPerPerson else "0"
            um_limit_occurrence = cov.limitPerOccurrence.replace(',', '') if cov.limitPerOccurrence else "0"

    # Vehicle-level coverages (physical damage deductibles for the first vehicle)
    coll_deductible = None
    comp_deductible = None
    
    vehicle_item_number = first_vehicle_graph.itemNumber
    physical_damage_coverages = [
        vc for vc in graph_data.vehicleCoverages 
        if vc.vehicleItemNumber == vehicle_item_number and vc.deductibleLimit
    ]
    
    if len(physical_damage_coverages) > 0:
        # Assumption: The first physical damage coverage with a deductible is Collision
        coll_deductible = int(physical_damage_coverages[0].deductibleLimit.replace(',', ''))
    if len(physical_damage_coverages) > 1:
        # Assumption: The second is Comprehensive
        comp_deductible = int(physical_damage_coverages[1].deductibleLimit.replace(',', ''))
        
    coverages = Coverages(
        BIPD=Coverage(limits=f"{bi_limit}/{pd_limit}"),
        COLL=Coverage(deductible=coll_deductible) if coll_deductible is not None else None,
        COMP=Coverage(deductible=comp_deductible) if comp_deductible is not None else None,
        MPC=Coverage(limits=mpc_limit) if mpc_limit is not None else None,
        UM=Coverage(limits=f"{um_limit_person}/{um_limit_occurrence}")
    )

    # 5. Map Discounts
    discount_names = {d.discountName for d in graph_data.discounts}
    discounts = Discounts(
        good_driver="Good Driver" in discount_names,
        good_student="Good Student" in discount_names,
        mature_driver_course="Mature Driver" in discount_names,
        multi_line="Multi Policy Home" in discount_names,
        # multi_car is handled by `usage.single_automobile`
    )

    # 6. Map Usage
    usage = Usage(
        annual_mileage=12000, # Default value
        single_automobile=len(graph_data.vehicles) == 1
    )

    # --- Construct the final RatingInput object ---
    rating_input = RatingInput(
        carrier="STATEFARM", # Hardcoded to StateFarm as requested
        state="CA", # Hardcoded as it's the only supported state
        zip_code=zip_code,
        vehicle=vehicle,
        coverages=coverages,
        drivers=drivers,
        discounts=discounts,
        special_factors=SpecialFactors(), # Using defaults
        usage=usage
    )
    
    return rating_input
