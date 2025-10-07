# services/data-quality-service/app/main.py
from typing import Optional, Dict, Any, List
import re
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Valid email regex used in all email validations
EMAIL_REGEX = re.compile(r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}$")

# ... other imports and code ...

class DataQualityService:
    # ... other methods ...

    async def _load_dataset(self, organization_id: str, dataset_name: str,
                            data_sample: Optional[Dict] = None) -> pd.DataFrame:
        """Load dataset from various sources."""
        if data_sample:
            # For real-time validation, use provided sample
            return pd.DataFrame([data_sample])
        try:
            # Try to load from S3 (assuming CSV format)
            bucket_name = f"multi-tenant-ingestion-{organization_id}-data"
            key = f"datasets/{dataset_name}.csv"
            response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            return pd.read_csv(response['Body'])
        except Exception as e:
            logger.warning("Failed to load dataset from S3",
                           dataset_name=dataset_name,
                           error=str(e))
            # Could try other sources here (Databricks, local files, etc.)
            return None

    def _is_valid_email(self, value: str) -> bool:
        if value is None:
            return False
        return EMAIL_REGEX.match(value) is not None

    def validate_conformity(self, df: pd.DataFrame, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate dataset conformity based on provided rules.
        Expected rule keys: type, column, operator, value, severity
        """
        results: List[Dict[str, Any]] = []
        for rule in rules:
            rtype = rule.get('type')
            column = rule.get('column')
            operator = rule.get('operator')
            target = rule.get('value')
            severity = rule.get('severity', 'error')

            # Skip invalid rules early
            if not rtype or not column or column not in df.columns:
                results.append({
                    'rule': rule,
                    'passed': False,
                    'message': 'Invalid rule or column not found',
                    'severity': 'error'
                })
                continue

            passed = True
            message = 'OK'

            try:
                series = df[column]
                if rtype == 'conformity:email':
                    passed = series.fillna('').map(self._is_valid_email).all()
                    if not passed:
                        message = 'Invalid email(s) found'
                elif rtype == 'conformity:type':
                    # Ensure dtype matches expected pandas dtype string
                    expected = str(target)
                    passed = str(series.dtype) == expected
                    if not passed:
                        message = f"Column dtype {series.dtype} != {expected}"
                elif rtype == 'conformity:range':
                    # numeric comparisons
                    if operator == 'between' and isinstance(target, (list, tuple)) and len(target) == 2:
                        lo, hi = target
                        passed = series.ge(lo).& series.le(hi).all()
                    elif operator == '>=':
                        passed = series.ge(target).all()
                    elif operator == '<=':
                        passed = series.le(target).all()
                    elif operator == '>' :
                        passed = series.gt(target).all()
                    elif operator == '<' :
                        passed = series.lt(target).all()
                    else:
                        passed = False
                        message = 'Unsupported range operator'
                    if not passed and message == 'OK':
                        message = 'Values out of range'
                elif rtype == 'conformity:in':
                    allowed = set(target if isinstance(target, (list, tuple, set)) else [target])
                    passed = series.isin(allowed).all()
                    if not passed:
                        message = 'Unexpected values present'
                else:
                    passed = False
                    message = 'Unsupported conformity rule type'
            except Exception as e:
                passed = False
                message = f'Validation error: {e}'

            results.append({
                'rule': rule,
                'passed': passed,
                'message': message,
                'severity': severity,
            })

        summary = {
            'total': len(results),
            'passed': sum(1 for r in results if r['passed']),
            'failed': sum(1 for r in results if not r['passed'] and r['severity'] == 'error'),
            'warnings': sum(1 for r in results if not r['passed'] and r['severity'] != 'error'),
            'results': results,
        }
        return summary

    def build_rules_query(self, request: Any) -> Dict[str, Any]:
        """Construct a rules query filter dict from request safely."""
        filters: Dict[str, Any] = {}
        # Use hasattr before accessing request attributes
        if hasattr(request, 'organization_id') and request.organization_id:
            filters['organization_id'] = request.organization_id
        if hasattr(request, 'dataset_name') and request.dataset_name:
            filters['dataset_name'] = request.dataset_name
        if hasattr(request, 'target_table') and getattr(request, 'target_table'):
            filters['target_table'] = request.target_table
        if hasattr(request, 'severity') and request.severity:
            filters['severity'] = request.severity
        return filters

# ... rest of file ...
