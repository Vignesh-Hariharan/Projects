#!/usr/bin/env python3

import json
from datetime import datetime
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class SnowflakeQualityWriter:
    def __init__(self, connection_params: Dict[str, str]):
        self.connection_params = connection_params
        self.connection = None
        
    def connect(self):
        try:
            import snowflake.connector
            
            self.connection = snowflake.connector.connect(
                user=self.connection_params['user'],
                password=self.connection_params['password'],
                account=self.connection_params['account'],
                warehouse=self.connection_params['warehouse'],
                database=self.connection_params['database'],
                schema=self.connection_params['schema']
            )
            logger.info("Connected to Snowflake warehouse")
            
        except ImportError:
            logger.warning("Snowflake connector not installed. Run: pip install snowflake-connector-python")
            return False
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            return False
            
        return True
    
    def create_quality_tables(self):
        if not self.connection:
            return False
            
        try:
            cursor = self.connection.cursor()
            
            # Create main quality metrics table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS DATA_QUALITY_METRICS (
                    RUN_ID VARCHAR(100) PRIMARY KEY,
                    TIMESTAMP TIMESTAMP_NTZ,
                    DATA_SOURCE VARCHAR(100),
                    SUCCESS_RATE DECIMAL(5,2),
                    TOTAL_CHECKS INTEGER,
                    PASSED_CHECKS INTEGER,
                    FAILED_CHECKS INTEGER,
                    OVERALL_STATUS VARCHAR(20),
                    METADATA VARIANT,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            
            # Create detailed validation results table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS DATA_QUALITY_RESULTS (
                    RESULT_ID INTEGER AUTOINCREMENT PRIMARY KEY,
                    RUN_ID VARCHAR(100),
                    RULE_NAME VARCHAR(50),
                    SUCCESS BOOLEAN,
                    SCORE DECIMAL(5,2),
                    DETAILS VARIANT,
                    RECOMMENDATIONS ARRAY,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    FOREIGN KEY (RUN_ID) REFERENCES DATA_QUALITY_METRICS(RUN_ID)
                )
            """)
            
            cursor.close()
            logger.info("✅ Data quality tables created/verified in Snowflake")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create tables: {e}")
            return False
    
    def write_quality_metrics(self, metrics: Dict[str, Any], validation_results: list = None):
        """
        Write quality metrics to Snowflake warehouse.
        
        Args:
            metrics: Quality metrics dictionary
            validation_results: List of individual validation results
        """
        if not self.connection:
            logger.warning("No Snowflake connection available")
            return False
            
        try:
            cursor = self.connection.cursor()
            
            # Insert main metrics
            cursor.execute("""
                INSERT INTO DATA_QUALITY_METRICS 
                (RUN_ID, TIMESTAMP, DATA_SOURCE, SUCCESS_RATE, TOTAL_CHECKS, 
                 PASSED_CHECKS, FAILED_CHECKS, OVERALL_STATUS, METADATA)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                metrics['run_id'],
                datetime.fromisoformat(metrics['timestamp'].replace('Z', '+00:00')),
                'marketing_data',
                metrics['success_rate'],
                metrics['total_checks'], 
                metrics['passed_checks'],
                metrics['failed_checks'],
                metrics['overall_status'],
                json.dumps(metrics)
            ))
            
            # Insert detailed results if provided
            if validation_results:
                for result in validation_results:
                    cursor.execute("""
                        INSERT INTO DATA_QUALITY_RESULTS
                        (RUN_ID, RULE_NAME, SUCCESS, SCORE, DETAILS, RECOMMENDATIONS)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        metrics['run_id'],
                        result.rule_name,
                        result.success,
                        result.score,
                        json.dumps(result.details),
                        json.dumps(result.recommendations)
                    ))
            
            cursor.close()
            logger.info(f"✅ Quality metrics written to Snowflake: {metrics['run_id']}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to write metrics to Snowflake: {e}")
            return False
    
    def get_quality_trends(self, days: int = 30) -> Optional[list]:
        """
        Retrieve quality trends for dashboard reporting.
        
        Args:
            days: Number of days to look back
            
        Returns:
            List of quality trend data for visualization
        """
        if not self.connection:
            return None
            
        try:
            cursor = self.connection.cursor()
            
            cursor.execute("""
                SELECT 
                    DATE(TIMESTAMP) as DATE,
                    AVG(SUCCESS_RATE) as AVG_SUCCESS_RATE,
                    COUNT(*) as TOTAL_RUNS,
                    SUM(CASE WHEN OVERALL_STATUS = 'CRITICAL' THEN 1 ELSE 0 END) as CRITICAL_RUNS
                FROM DATA_QUALITY_METRICS
                WHERE TIMESTAMP >= DATEADD(day, -%s, CURRENT_TIMESTAMP())
                GROUP BY DATE(TIMESTAMP)
                ORDER BY DATE DESC
            """, (days,))
            
            results = cursor.fetchall()
            cursor.close()
            
            return [
                {
                    'date': str(row[0]),
                    'avg_success_rate': float(row[1]),
                    'total_runs': int(row[2]), 
                    'critical_runs': int(row[3])
                }
                for row in results
            ]
            
        except Exception as e:
            logger.error(f"❌ Failed to retrieve quality trends: {e}")
            return None
    
    def close(self):
        """Close Snowflake connection"""
        if self.connection:
            self.connection.close()
            logger.info("Snowflake connection closed")