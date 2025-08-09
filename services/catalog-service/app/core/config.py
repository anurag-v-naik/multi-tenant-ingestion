from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    DATABASE_URL: str
    UNITY_CATALOG_ENABLED: bool = True
    DATABRICKS_HOST: str
    DATABRICKS_TOKEN: str
    ALLOWED_ORIGINS: List[str] = ["*"]

    class Config:
        env_file = ".env"


settings = Settings()