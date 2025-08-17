class HealthService:
    """
    Service layer for health-related logic.
    """

    def get_health_status(self) -> dict[str, str]:
        """
        Performs the health check logic.
        In a real application, this could involve checking database connections, etc.
        """
        return {"status": "ok"}

def get_health_service() -> HealthService:
    return HealthService()