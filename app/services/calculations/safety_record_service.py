import pandas as pd
from datetime import date, timedelta
from typing import List, Dict, Tuple
from math import floor

from app.utils.data_loader import DataLoader
from app.models.models import Driver, Violation


class SafetyRecordService:
    """
    Service for calculating time-decayed safety record scores based on violations and accidents.
    
    The safety record score starts at 8 (clean record) and increases based on violations.
    Each violation type has:
    - Initial points added
    - Decay period (years until completely removed)
    - Decay amount (points reduced per full year since incident)
    """
    
    def __init__(self, carrier_config: dict):
        self.carrier_config = carrier_config
        self.data_loader = DataLoader()
        self.violation_scores: pd.DataFrame = None
        
    def initialize(self):
        """Loads the violation scoring table into memory."""
        self.violation_scores = self.data_loader.load_safety_driver_record_score()
        
    def calculate_safety_record_level(self, driver: Driver, assessment_date: date = None) -> int:
        """
        Calculates the driver's safety record level based on violations with time decay.
        
        Args:
            driver: Driver with violations list
            assessment_date: Date to assess score (defaults to today)
            
        Returns:
            Safety record level (0-30, where 8 is clean record)
        """
        if self.violation_scores is None:
            self.initialize()
            
        if assessment_date is None:
            assessment_date = date.today()
            
        # Clean driver starts at level 0
        additional_points = 0
        
        # Calculate current points from all violations
        for violation in driver.violations:
            current_points = self._calculate_current_violation_points(
                violation, assessment_date
            )
            additional_points += current_points
            
        # Safety record level is just the violation points (0 for clean driver)
        final_level = int(additional_points)
        
        # Cap at maximum level 30
        return min(final_level, 30)
    
    def _calculate_current_violation_points(self, violation: Violation, assessment_date: date) -> float:
        """
        Calculates current points for a single violation considering time decay.
        
        Args:
            violation: The violation to assess
            assessment_date: Date to assess from
            
        Returns:
            Current points after decay (0 if fully decayed)
        """
        # Get violation parameters from the scoring table
        violation_row = self.violation_scores[
            self.violation_scores['violation_type'] == violation.type
        ]
        
        if violation_row.empty:
            # Unknown violation type, return 0 points
            return 0.0
            
        row = violation_row.iloc[0]
        initial_points = int(row['points_added'])
        decay_period_years = int(row['decay_period'])
        decay_amount_per_year = float(row['decay_amounts'])
        
        # Calculate years since violation
        years_since = (assessment_date - violation.date).days / 365.25
        full_years_since = floor(years_since)
        
        # If past decay period, violation is completely removed
        if full_years_since >= decay_period_years:
            return 0.0
            
        # Calculate current points after decay
        points_decayed = full_years_since * decay_amount_per_year
        current_points = max(0.0, initial_points - points_decayed)
        
        return current_points
    
    def get_violation_details(self, driver: Driver, assessment_date: date = None) -> Dict:
        """
        Gets detailed breakdown of violation points and decay for a driver.
        
        Args:
            driver: Driver with violations list
            assessment_date: Date to assess from (defaults to today)
            
        Returns:
            Detailed breakdown of violation scoring
        """
        if assessment_date is None:
            assessment_date = date.today()
            
        violation_details = []
        total_current_points = 0
        
        for violation in driver.violations:
            current_points = self._calculate_current_violation_points(violation, assessment_date)
            
            # Get violation parameters
            violation_row = self.violation_scores[
                self.violation_scores['violation_type'] == violation.type
            ]
            
            if not violation_row.empty:
                row = violation_row.iloc[0]
                years_since = (assessment_date - violation.date).days / 365.25
                full_years_since = floor(years_since)
                
                detail = {
                    'violation_type': violation.type,
                    'violation_date': violation.date.isoformat(),
                    'years_since': round(years_since, 2),
                    'full_years_since': full_years_since,
                    'initial_points': int(row['points_added']),
                    'decay_period': int(row['decay_period']),
                    'decay_per_year': float(row['decay_amounts']),
                    'current_points': round(current_points, 2),
                    'fully_removed': current_points == 0.0
                }
                violation_details.append(detail)
                total_current_points += current_points
        
        base_score = 0  # Clean driver starts at 0
        final_level = min(int(total_current_points), 30)
        
        return {
            'driver_id': driver.id,
            'assessment_date': assessment_date.isoformat(),
            'base_score': base_score,
            'total_violation_points': round(total_current_points, 2),
            'final_safety_level': final_level,
            'violations': violation_details,
            'clean_record': len(violation_details) == 0 or total_current_points == 0
        }
    
    def simulate_future_scores(self, driver: Driver, years_ahead: int = 5) -> List[Dict]:
        """
        Simulates how the driver's safety record will change over time as violations decay.
        
        Args:
            driver: Driver with violations list
            years_ahead: Number of years to simulate ahead
            
        Returns:
            List of safety scores by year
        """
        today = date.today()
        future_scores = []
        
        for year in range(years_ahead + 1):
            future_date = date(today.year + year, today.month, today.day)
            level = self.calculate_safety_record_level(driver, future_date)
            details = self.get_violation_details(driver, future_date)
            
            future_scores.append({
                'year': year,
                'date': future_date.isoformat(),
                'safety_level': level,
                'violation_points': details['total_violation_points'],
                'clean_record': details['clean_record']
            })
            
        return future_scores
