from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import HTTPBearer
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import logging
import httpx
import os

from .connectors.postgresql import PostgreSQLConnector
from .connectors.base import ConnectorError

app = FastAPI(
    title="Multi-Tenant Connector Service",
    description="Data source connectivity service with tenant isolation",
    version="1.0.0"
)

security = HTTPBearer()
AUTH_SERVICE_URL = os.getenv("AUTH_SERVICE_URL", "http://auth-service:8000")


# Pydantic models
class ConnectorConfig(BaseModel):
    connector_type: str
    config: Dict[str, Any]


class QueryRequest(BaseModel):
    query: str
    limit: Optional[int] = None


class ConnectorResponse(BaseModel):
    connector_id: str
    status: str
    message: str


# Global connector registry
connectors: Dict[str, Dict[str, Any]] = {}

# Connector factory
CONNECTOR_TYPES = {
    "postgresql": PostgreSQLConnector,
    # Add more connector types here
    # "mysql": MySQLConnector,
    # "mongodb": MongoDBConnector,
}


async def verify_tenant_token(credentials=Depends(security)):
    """Verify JWT token and extract tenant information"""
    try:
        async with httpx.AsyncClient() as client:
            headers = {"Authorization": f"Bearer {credentials.credentials}"}
            response = await client.get(f"{AUTH_SERVICE_URL}/auth/validate", headers=headers)

            if response.status_code != 200:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid authentication token"
                )

            return response.json()
    except httpx.RequestError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service unavailable"
        )


@app.post("/connectors", response_model=ConnectorResponse)
async def create_connector(
        connector_config: ConnectorConfig,
        user_info: dict = Depends(verify_tenant_token)
):
    """Create a new data connector for the tenant"""
    tenant_id = user_info["tenant_id"]
    connector_type = connector_config.connector_type.lower()

    if connector_type not in CONNECTOR_TYPES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported connector type: {connector_type}"
        )

    try:
        # Create connector instance
        connector_class = CONNECTOR_TYPES[connector_type]
        connector = connector_class(connector_config.config, tenant_id)

        # Test connection
        if not connector.connect():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to connect to data source"
            )

        # Store connector
        connector_id = f"{tenant_id}_{connector_type}_{len(connectors.get(tenant_id, {}))}"
        if tenant_id not in connectors:
            connectors[tenant_id] = {}

        connectors[tenant_id][connector_id] = {
            "connector": connector,
            "config": connector_config.config,
            "type": connector_type
        }

        return ConnectorResponse(
            connector_id=connector_id,
            status="created",
            message=f"Connector created successfully"
        )

    except ConnectorError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@app.get("/connectors")
async def list_connectors(user_info: dict = Depends(verify_tenant_token)):
    """List all connectors for the tenant"""
    tenant_id = user_info["tenant_id"]

    if tenant_id not in connectors:
        return {"connectors": []}

    connector_list = []
    for connector_id, connector_info in connectors[tenant_id].items():
        connector_list.append({
            "connector_id": connector_id,
            "type": connector_info["type"],
            "metadata": connector_info["connector"].get_metadata()
        })

    return {"connectors": connector_list}


@app.post("/connectors/{connector_id}/query")
async def execute_query(
        connector_id: str,
        query_request: QueryRequest,
        user_info: dict = Depends(verify_tenant_token)
):
    """Execute query on specified connector"""
    tenant_id = user_info["tenant_id"]

    if (tenant_id not in connectors or
            connector_id not in connectors[tenant_id]):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connector not found"
        )

    try:
        connector = connectors[tenant_id][connector_id]["connector"]
        results = connector.extract_data(query_request.query, query_request.limit)

        return {
            "connector_id": connector_id,
            "query": query_request.query,
            "row_count": len(results),
            "data": results
        }

    except ConnectorError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@app.get("/connectors/{connector_id}/schema")
async def get_connector_schema(
        connector_id: str,
        table_name: Optional[str] = None,
        user_info: dict = Depends(verify_tenant_token)
):
    """Get schema information from connector"""
    tenant_id = user_info["tenant_id"]

    if (tenant_id not in connectors or
            connector_id not in connectors[tenant_id]):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connector not found"
        )

    try:
        connector = connectors[tenant_id][connector_id]["connector"]
        schema = connector.get_schema(table_name)

        return {
            "connector_id": connector_id,
            "schema": schema
        }

    except ConnectorError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@app.get("/connectors/{connector_id}/tables")
async def get_connector_tables(
        connector_id: str,
        user_info: dict = Depends(verify_tenant_token)
):
    """Get list of tables from connector"""
    tenant_id = user_info["tenant_id"]

    if (tenant_id not in connectors or
            connector_id not in connectors[tenant_id]):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connector not found"
        )

    try:
        connector = connectors[tenant_id][connector_id]["connector"]
        tables = connector.get_table_list()

        return {
            "connector_id": connector_id,
            "tables": tables
        }

    except ConnectorError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@app.get("/connectors/{connector_id}/health")
async def check_connector_health(
        connector_id: str,
        user_info: dict = Depends(verify_tenant_token)
):
    """Check health status of connector"""
    tenant_id = user_info["tenant_id"]

    if (tenant_id not in connectors or
            connector_id not in connectors[tenant_id]):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connector not found"
        )

    connector = connectors[tenant_id][connector_id]["connector"]
    health_status = connector.health_check()

    return {
        "connector_id": connector_id,
        "health": health_status
    }


@app.delete("/connectors/{connector_id}")
async def delete_connector(
        connector_id: str,
        user_info: dict = Depends(verify_tenant_token)
):
    """Delete a connector"""
    tenant_id = user_info["tenant_id"]

    if (tenant_id not in connectors or
            connector_id not in connectors[tenant_id]):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connector not found"
        )

    # Disconnect and remove connector
    connector = connectors[tenant_id][connector_id]["connector"]
    connector.disconnect()
    del connectors[tenant_id][connector_id]

    return {"message": f"Connector {connector_id} deleted successfully"}


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "connector-service"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
