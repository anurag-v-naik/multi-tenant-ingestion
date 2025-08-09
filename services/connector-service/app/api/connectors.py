from fastapi import APIRouter, HTTPException, status, Depends
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from ..core.database import get_db
from ..core.registry import connector_registry
from ..utils.auth import get_current_user
from ..models.user import User

router = APIRouter()


class ConnectorInfo(BaseModel):
    name: str
    type: str
    supported_formats: List[str]
    config_schema: Dict[str, Any]
    credential_schema: Dict[str, Any]


class TestConnectionRequest(BaseModel):
    connector_type: str
    config: Dict[str, Any]
    credentials: Dict[str, Any]


@router.get("/", response_model=List[ConnectorInfo])
async def list_connectors(
        current_user: User = Depends(get_current_user),
        db: Session = Depends(get_db)
):
    """List all available connectors with their schemas"""

    connectors = []
    for connector_data in connector_registry.list_connectors():
        connector_class = connector_registry.get_connector(connector_data["name"])

        connectors.append(ConnectorInfo(
            name=connector_data["name"],
            type=connector_data["type"],
            supported_formats=connector_data["supported_formats"],
            config_schema=connector_class.get_config_schema(),
            credential_schema=connector_class.get_credential_schema()
        ))

    return connectors


@router.get("/{connector_name}", response_model=ConnectorInfo)
async def get_connector_info(
        connector_name: str,
        current_user: User = Depends(get_current_user),
        db: Session = Depends(get_db)
):
    """Get detailed information about a specific connector"""

    try:
        connector_class = connector_registry.get_connector(connector_name)
        connector_info = next(
            (c for c in connector_registry.list_connectors() if c["name"] == connector_name),
            None
        )

        if not connector_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Connector '{connector_name}' not found"
            )

        return ConnectorInfo(
            name=connector_info["name"],
            type=connector_info["type"],
            supported_formats=connector_info["supported_formats"],
            config_schema=connector_class.get_config_schema(),
            credential_schema=connector_class.get_credential_schema()
        )

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )


@router.post("/test-connection")
async def test_connection(
        request: TestConnectionRequest,
        current_user: User = Depends(get_current_user),
        db: Session = Depends(get_db)
):
    """Test a connection with the provided configuration"""

    try:
        connector = connector_registry.create_connector(
            request.connector_type,
            request.config,
            request.credentials
        )

        # Validate configuration first
        validation_result = await connector.validate_config(request.config)
        if not validation_result.is_valid:
            return {
                "success": False,
                "message": f"Configuration validation failed: {validation_result.message}",
                "validation_error": True
            }

        # Test connection
        test_result = await connector.test_connection()
        await connector.disconnect()

        return {
            "success": test_result.success,
            "message": test_result.message,
            "latency_ms": test_result.latency_ms,
            "metadata": test_result.metadata
        }

    except Exception as e:
        return {
            "success": False,
            "message": f"Connection test failed: {str(e)}"
        }


@router.post("/{connector_name}/preview")
async def preview_data(
        connector_name: str,
        config: Dict[str, Any],
        credentials: Dict[str, Any],
        limit: int = 100,
        current_user: User = Depends(get_current_user),
        db: Session = Depends(get_db)
):
    """Preview data from a connector"""

    try:
        connector = connector_registry.create_connector(
            connector_name,
            config,
            credentials
        )

        await connector.connect()
        preview = await connector.preview_data(limit=limit)
        await connector.disconnect()

        return {
            "columns": preview.columns,
            "rows": preview.rows,
            "total_rows": preview.total_rows,
            "sample_size": preview.sample_size
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to preview data: {str(e)}"
        )


@router.post("/{connector_name}/schema")
async def get_schema(
        connector_name: str,
        config: Dict[str, Any],
        credentials: Dict[str, Any],
        current_user: User = Depends(get_current_user),
        db: Session = Depends(get_db)
):
    """Get schema/metadata from a connector"""

    try:
        connector = connector_registry.create_connector(
            connector_name,
            config,
            credentials
        )

        await connector.connect()
        schema = await connector.get_schema()
        await connector.disconnect()

        return schema

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to get schema: {str(e)}"
        )