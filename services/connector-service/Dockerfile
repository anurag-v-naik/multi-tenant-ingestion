FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/

# Expose port
EXPOSE 8002

# Run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8002", "--reload"]
                'name': table_name,
                'columns': []
            }

            for col in columns_result:
                tables[table_name]['columns'].append({
                    'name': col[0],  # Field
                    'type': col[1],  # Type
                    'nullable': col[2] == 'YES',  # Null
                    'key': col[3],   # Key
                    'default': col[4],  # Default
                    'extra': col[5]  # Extra
                })

        await cursor.close()

        return {
            'database': self.config['database'],
            'tables': list(tables.values())
        }

    async def preview_data(self, limit: int = 100, table_name: str = None) -> DataPreview:
        if not self._connection:
            await self.connect()

        cursor = await self._connection.cursor()

        if not table_name:
            await cursor.execute("SHOW TABLES")
            tables = await cursor.fetchall()
            if not tables:
                raise ValueError("No tables found")
            table_name = tables[0][0]

        # Get column info
        await cursor.execute(f"DESCRIBE `{table_name}`")
        columns_info = await cursor.fetchall()

        # Get sample data
        await cursor.execute(f"SELECT * FROM `{table_name}` LIMIT %s", (limit,))
        rows = await cursor.fetchall()

        # Get column names
        column_names = [desc[0] for desc in cursor.description]

        # Get total count
        await cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        total_rows = (await cursor.fetchone())[0]

        await cursor.close()

        return DataPreview(
            columns=[{'name': col[0], 'type': col[1]} for col in columns_info],
            rows=[dict(zip(column_names, row)) for row in rows],
            total_rows=total_rows,
            sample_size=len(rows)
        )

    async def read_data(self, query: Optional[str] = None, table_name: str = None, **kwargs) -> List[Dict[str, Any]]:
        if not self._connection:
            await self.connect()

        cursor = await self._connection.cursor()

        if query:
            await cursor.execute(query)
        elif table_name:
            await cursor.execute(f"SELECT * FROM `{table_name}`")
        else:
            raise ValueError("Either query or table_name must be provided")

        rows = await cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        await cursor.close()

        return [dict(zip(column_names, row)) for row in rows]
