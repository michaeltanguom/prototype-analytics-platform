"""
Soda Core Data Quality Health Check Reporter with Tiered Validation
Implements sequential execution: Critical -> Quality -> Monitoring
Returns tiered validation status: CRITICAL_FAIL, QUALITY_FAIL, WARNING, PASS

Usage - run in the terminal command line: python soda_dq.py <staging_table> [config_file] [checks_dir] [db_file_path] [output_dir]"
Example: python soda_dq.py silver_stg_scopus"
Example: python soda_dq.py silver_stg_scopus soda_configuration.yml checks scopus_test.db dq_reports/"
"""

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import logging
import importlib.metadata

from soda.scan import Scan

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TieredSodaHealthCheckReporter:
    """Class to run tiered Soda checks and generate comprehensive health check reports"""
    
    def __init__(self, config_file: str, checks_dir: str, staging_table: str, 
                 db_file_path: str = "scopus_test.db", datasource_name: str = "scopus_duckdb"):
        """
        Initialise the tiered reporter
        
        Args:
            config_file: Path to Soda configuration file
            checks_dir: Directory containing tiered check files
            staging_table: Name of the staging table to scan
            db_file_path: Path to the DuckDB database file
            datasource_name: Name of the data source in configuration
        """
        self.config_file = Path(config_file)
        self.checks_dir = Path(checks_dir)
        self.staging_table = staging_table
        self.db_file_path = db_file_path
        self.datasource_name = datasource_name
        self.load_id = None
        
        # Tiered check results
        self.critical_results = None
        self.quality_results = None
        self.monitoring_results = None
        
        # Check file paths
        self.critical_checks_file = self.checks_dir / "scopus_critical_checks.yml"
        self.quality_checks_file = self.checks_dir / "scopus_quality_checks.yml"
        self.monitoring_checks_file = self.checks_dir / "scopus_monitoring_checks.yml"
        
        # Validate inputs
        self._validate_setup()
    
    def _validate_setup(self) -> None:
        """Validate that all required files exist"""
        if not self.config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_file}")
        if not self.checks_dir.exists():
            raise FileNotFoundError(f"Checks directory not found: {self.checks_dir}")
        if not Path(self.db_file_path).exists():
            raise FileNotFoundError(f"Database file not found: {self.db_file_path}")
        
        # Check for tiered check files
        missing_files = []
        if not self.critical_checks_file.exists():
            missing_files.append(str(self.critical_checks_file))
        if not self.quality_checks_file.exists():
            missing_files.append(str(self.quality_checks_file))
        if not self.monitoring_checks_file.exists():
            missing_files.append(str(self.monitoring_checks_file))
        
        if missing_files:
            logger.warning(f"Missing tiered check files: {missing_files}")
            logger.warning("Will fall back to all .yml files in checks directory")
    
    def get_soda_version(self) -> str:
        """Get Soda Core version dynamically"""
        try:
            return importlib.metadata.version('soda-core-duckdb')
        except importlib.metadata.PackageNotFoundError:
            try:
                return importlib.metadata.version('soda-core')
            except importlib.metadata.PackageNotFoundError:
                return "unknown"

    def extract_load_id(self) -> Optional[str]:
        """Extract load_id from staging table"""
        try:
            import duckdb
            
            logger.info(f"Extracting load_id from staging table: {self.staging_table}")
            
            conn = duckdb.connect(self.db_file_path)
            query = f"SELECT DISTINCT load_id FROM {self.staging_table} LIMIT 1"
            result = conn.execute(query).fetchone()
            conn.close()
            
            if result and result[0] is not None:
                load_id = str(result[0])
                logger.info(f"🔗 Successfully extracted load_id: {load_id}")
                return load_id
            else:
                logger.warning(f"No load_id found in staging table")
                return None
                
        except Exception as e:
            logger.error(f"Failed to extract load_id: {e}")
            return None

    def get_staging_table_info(self) -> Dict[str, Any]:
        """Get basic information about the staging table"""
        try:
            import duckdb
            
            conn = duckdb.connect(self.db_file_path)
            
            row_count = conn.execute(f"SELECT COUNT(*) FROM {self.staging_table}").fetchone()[0]
            columns_result = conn.execute(f"DESCRIBE {self.staging_table}").fetchall()
            columns = [col[0] for col in columns_result]
            
            conn.close()
            
            table_info = {
                'table_name': self.staging_table,
                'row_count': row_count,
                'columns': columns,
                'has_load_id_column': 'load_id' in columns,
                'total_columns': len(columns)
            }
            
            logger.info(f"Table info: {row_count} rows, {len(columns)} columns")
            return table_info
            
        except Exception as e:
            logger.error(f"Failed to get table info: {e}")
            return {}

    def run_single_tier_scan(self, check_file: Path, tier_name: str) -> Dict[str, Any]:
        """Run Soda scan for a single tier of checks"""
        logger.info(f"Running {tier_name} checks from {check_file}")
        
        if not check_file.exists():
            logger.warning(f"Check file not found: {check_file}")
            return self._create_empty_scan_result()
        
        try:
            scan = Scan()
            scan.set_data_source_name(self.datasource_name)
            scan.add_configuration_yaml_file(str(self.config_file))
            scan.add_sodacl_yaml_file(str(check_file))
            
            logger.info(f"⚡ Executing {tier_name} checks on {self.staging_table}")
            scan.execute()
            
            scan_results = scan.get_scan_results()
            
            # Parse results
            checks = scan_results.get('checks', [])
            passed = len([c for c in checks if c.get('outcome') == 'pass'])
            failed = len([c for c in checks if c.get('outcome') == 'fail'])
            errors = len([c for c in checks if c.get('outcome') == 'error'])
            warnings = len([c for c in checks if c.get('outcome') == 'warn'])
            
            logger.info(f"{tier_name} results: {passed} passed, {failed} failed, {errors} errors, {warnings} warnings")
            
            return {
                'scan_results': scan_results,
                'tier_name': tier_name,
                'check_file': str(check_file),
                'total_checks': len(checks),
                'passed_checks': passed,
                'failed_checks': failed,
                'error_checks': errors,
                'warning_checks': warnings,
                'checks': checks
            }
            
        except Exception as e:
            logger.error(f"Failed to run {tier_name} checks: {e}")
            return self._create_empty_scan_result(tier_name, str(check_file), error=str(e))
    
    def _create_empty_scan_result(self, tier_name: str = "unknown", check_file: str = "", error: str = None) -> Dict[str, Any]:
        """Create empty scan result structure"""
        return {
            'scan_results': {},
            'tier_name': tier_name,
            'check_file': check_file,
            'total_checks': 0,
            'passed_checks': 0,
            'failed_checks': 0,
            'error_checks': 0,
            'warning_checks': 0,
            'checks': [],
            'error': error
        }

    def run_tiered_scan(self) -> Tuple[str, Dict[str, Any]]:
        """
        Execute tiered scan: Critical -> Quality -> Monitoring
        Returns: (overall_status, combined_results)
        """
        logger.info("Starting Tiered Soda Core data quality scan...")
        
        # Extract load_id first - THIS IS CRITICAL FOR PROPER FILENAME GENERATION
        self.load_id = self.extract_load_id()
        if not self.load_id:
            logger.warning("No load_id found - report filename will not include load_id")
        
        # Step 1: Run critical checks first
        logger.info("PHASE 1: Running Critical Checks")
        self.critical_results = self.run_single_tier_scan(self.critical_checks_file, "CRITICAL")
        
        # Check if critical checks failed
        critical_status = self._evaluate_tier_status(self.critical_results)
        if critical_status == "FAIL":
            logger.error("CRITICAL CHECKS FAILED - Stopping pipeline")
            return "CRITICAL_FAIL", self._create_combined_results(stop_reason="critical_failure")
        
        logger.info("Critical checks passed - continuing to quality checks")
        
        # Step 2: Run quality checks
        logger.info("PHASE 2: Running Quality Checks")
        self.quality_results = self.run_single_tier_scan(self.quality_checks_file, "QUALITY")
        
        quality_status = self._evaluate_tier_status(self.quality_results)
        if quality_status == "FAIL":
            logger.warning("Quality checks failed - continuing to monitoring checks")
        
        # Step 3: Run monitoring checks (always run for trend analysis)
        logger.info("PHASE 3: Running Monitoring Checks")
        self.monitoring_results = self.run_single_tier_scan(self.monitoring_checks_file, "MONITORING")
        
        # Determine overall status using decision matrix
        overall_status = self._determine_overall_status(critical_status, quality_status, self.monitoring_results)
        
        logger.info(f"Overall validation status: {overall_status}")
        
        return overall_status, self._create_combined_results()
    
    def _evaluate_tier_status(self, tier_results: Dict[str, Any]) -> str:
        """Evaluate if a tier passed or failed"""
        failed_checks = tier_results.get('failed_checks', 0)
        error_checks = tier_results.get('error_checks', 0)
        
        if failed_checks > 0 or error_checks > 0:
            return "FAIL"
        return "PASS"
    
    def _determine_overall_status(self, critical_status: str, quality_status: str, monitoring_results: Dict[str, Any]) -> str:
        """Apply decision matrix to determine overall status"""
        # Critical failures always result in CRITICAL_FAIL
        if critical_status == "FAIL":
            return "CRITICAL_FAIL"
        
        # Quality failures result in QUALITY_FAIL
        if quality_status == "FAIL":
            return "QUALITY_FAIL"
        
        # Check monitoring results for warnings
        monitoring_warnings = monitoring_results.get('warning_checks', 0)
        monitoring_failures = monitoring_results.get('failed_checks', 0) + monitoring_results.get('error_checks', 0)
        
        if monitoring_failures > 0:
            return "QUALITY_FAIL"  # Monitoring failures indicate quality issues
        elif monitoring_warnings > 0:
            return "WARNING"  # Warnings only
        else:
            return "PASS"  # All checks passed
    
    def _create_combined_results(self, stop_reason: str = None) -> Dict[str, Any]:
        """Create combined results from all tiers"""
        table_info = self.get_staging_table_info()
        
        # Aggregate metrics
        total_checks = 0
        total_passed = 0
        total_failed = 0
        total_errors = 0
        total_warnings = 0
        
        tiers = []
        
        if self.critical_results:
            tiers.append(self.critical_results)
            total_checks += self.critical_results.get('total_checks', 0)
            total_passed += self.critical_results.get('passed_checks', 0)
            total_failed += self.critical_results.get('failed_checks', 0)
            total_errors += self.critical_results.get('error_checks', 0)
            total_warnings += self.critical_results.get('warning_checks', 0)
        
        if self.quality_results:
            tiers.append(self.quality_results)
            total_checks += self.quality_results.get('total_checks', 0)
            total_passed += self.quality_results.get('passed_checks', 0)
            total_failed += self.quality_results.get('failed_checks', 0)
            total_errors += self.quality_results.get('error_checks', 0)
            total_warnings += self.quality_results.get('warning_checks', 0)
        
        if self.monitoring_results:
            tiers.append(self.monitoring_results)
            total_checks += self.monitoring_results.get('total_checks', 0)
            total_passed += self.monitoring_results.get('passed_checks', 0)
            total_failed += self.monitoring_results.get('failed_checks', 0)
            total_errors += self.monitoring_results.get('error_checks', 0)
            total_warnings += self.monitoring_results.get('warning_checks', 0)
        
        pass_rate = (total_passed / total_checks * 100) if total_checks > 0 else 0
        
        return {
            'metadata': {
                'report_type': 'tiered_soda_data_quality_health_check',
                'generated_at': datetime.now().isoformat(),
                'datasource': self.datasource_name,
                'staging_table': self.staging_table,
                'database_file': self.db_file_path,
                'load_id': self.load_id,
                'config_file': str(self.config_file),
                'checks_directory': str(self.checks_dir),
                'soda_version': self.get_soda_version(),
                'stop_reason': stop_reason
            },
            'lineage': {
                'load_id': self.load_id,
                'staging_table': self.staging_table,
                'database_file': self.db_file_path,
                'scan_timestamp': datetime.now().isoformat(),
                'datasource': self.datasource_name,
                'table_row_count': table_info.get('row_count', 0),
                'table_columns': table_info.get('total_columns', 0)
            },
            'summary': {
                'total_checks': total_checks,
                'passed_checks': total_passed,
                'failed_checks': total_failed,
                'error_checks': total_errors,
                'warning_checks': total_warnings,
                'pass_rate_percentage': round(pass_rate, 2),
                'staging_table_info': table_info,
                'tiers_executed': len(tiers)
            },
            'tier_results': {
                'critical': self.critical_results,
                'quality': self.quality_results,
                'monitoring': self.monitoring_results
            }
        }
    
    def create_tiered_health_check_report(self, overall_status: str, combined_results: Dict[str, Any]) -> Dict[str, Any]:
        """Create comprehensive tiered health check report with simplified tier results"""
        
        # Extract failed and error checks from all tiers
        all_failed_checks = []
        all_error_checks = []
        all_warning_checks = []
        
        # Simplified tier results - just passed check names
        simplified_tier_results = {
            'critical': {
                'passed_checks_summary': {
                    'count': 0,
                    'names': []
                },
                'failed_checks': [],
                'error_checks': []
            },
            'quality': {
                'passed_checks_summary': {
                    'count': 0,
                    'names': []
                },
                'failed_checks': [],
                'error_checks': []
            },
            'monitoring': {
                'passed_checks_summary': {
                    'count': 0,
                    'names': []
                },
                'failed_checks': [],
                'error_checks': []
            }
        }
        
        for tier_name, tier_result in combined_results['tier_results'].items():
            if tier_result and tier_result.get('checks'):
                passed_names = []
                failed_checks_for_tier = []
                error_checks_for_tier = []
                
                for check in tier_result['checks']:
                    check_name = check.get('name', 'Unknown')
                    outcome = check.get('outcome', 'unknown')
                    
                    if outcome == 'pass':
                        passed_names.append(check_name)
                    elif outcome == 'fail':
                        check_detail = {
                            'name': check_name,
                            'outcome': outcome,
                            'tier': tier_name,
                            'table': check.get('table', self.staging_table),
                            'column': check.get('column'),
                            'check_value': check.get('checkValue') or check.get('actualValue'),
                            'diagnostics': check.get('diagnostics', {})
                        }
                        failed_checks_for_tier.append(check_detail)
                        all_failed_checks.append(check_detail)
                    elif outcome == 'error':
                        check_detail = {
                            'name': check_name,
                            'outcome': outcome,
                            'tier': tier_name,
                            'table': check.get('table', self.staging_table),
                            'column': check.get('column'),
                            'check_value': check.get('checkValue') or check.get('actualValue'),
                            'diagnostics': check.get('diagnostics', {})
                        }
                        error_checks_for_tier.append(check_detail)
                        all_error_checks.append(check_detail)
                    elif outcome == 'warn':
                        check_detail = {
                            'name': check_name,
                            'outcome': outcome,
                            'tier': tier_name,
                            'table': check.get('table', self.staging_table),
                            'column': check.get('column'),
                            'check_value': check.get('checkValue') or check.get('actualValue'),
                            'diagnostics': check.get('diagnostics', {})
                        }
                        all_warning_checks.append(check_detail)
                
                # Update simplified tier results
                if tier_name in simplified_tier_results:
                    simplified_tier_results[tier_name]['passed_checks_summary'] = {
                        'count': len(passed_names),
                        'names': passed_names
                    }
                    simplified_tier_results[tier_name]['failed_checks'] = failed_checks_for_tier
                    simplified_tier_results[tier_name]['error_checks'] = error_checks_for_tier
        
        # Create comprehensive report with simplified structure
        report = {
            **combined_results,
            'tiered_summary': {
                'overall_status': overall_status,
                'critical_status': 'PASS' if self.critical_results and self._evaluate_tier_status(self.critical_results) == 'PASS' else 'FAIL',
                'quality_status': 'PASS' if self.quality_results and self._evaluate_tier_status(self.quality_results) == 'PASS' else 'FAIL',
                'monitoring_warnings': self.monitoring_results.get('warning_checks', 0) if self.monitoring_results else 0,
                'total_issues': len(all_failed_checks) + len(all_error_checks),
                'pipeline_blocking': overall_status == 'CRITICAL_FAIL'
            },
            # Replace complex tier_results with simplified version
            'tier_results': simplified_tier_results,
            'failed_checks': all_failed_checks,
            'error_checks': all_error_checks,
            'warning_checks': all_warning_checks,
            'passed_checks_summary': {
                'count': combined_results['summary']['passed_checks'],
                'by_tier': {
                    'critical': simplified_tier_results['critical']['passed_checks_summary']['count'],
                    'quality': simplified_tier_results['quality']['passed_checks_summary']['count'],
                    'monitoring': simplified_tier_results['monitoring']['passed_checks_summary']['count']
                }
            }
        }
        
        return report
    
    def save_report_json(self, report: Dict[str, Any], output_dir: str = "./dq_reports") -> str:
        """Save tiered health check report to JSON file with correct naming pattern"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d")
        
        # FIXED: Ensure load_id is properly included in filename when available
        if self.load_id:
            load_id_suffix = f"_load_{self.load_id}"
        else:
            load_id_suffix = ""
            logger.warning("No load_id available - filename will not include load_id component")
        
        table_suffix = f"_{self.staging_table}"
        
        # Use the correct naming pattern expected by the pipeline
        filename = f"soda_dq{table_suffix}{load_id_suffix}_{timestamp}.json"
        file_path = output_path / filename
        
        # Handle multiple reports on same day
        counter = 1
        while file_path.exists():
            filename = f"soda_dq{table_suffix}{load_id_suffix}_{timestamp}_{counter:02d}.json"
            file_path = output_path / filename
            counter += 1
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"Tiered health check report saved: {file_path}")
            logger.info(f"Filename pattern: soda_dq{table_suffix}{load_id_suffix}_{timestamp}.json")
            
            # ADDED: Explicit validation of filename pattern for debugging
            if self.load_id and load_id_suffix not in str(file_path):
                logger.error(f"FILENAME ERROR: Expected load_id '{self.load_id}' not found in filename: {file_path}")
            else:
                logger.info(f"Filename validation passed: load_id properly included")
            
            return str(file_path)
            
        except Exception as e:
            logger.error(f"Failed to save report: {e}")
            raise
    
    def generate_tiered_summary_report(self, report: Dict[str, Any]) -> str:
        """Generate human-readable summary of tiered health check"""
        metadata = report['metadata']
        lineage = report['lineage']
        summary = report['summary']
        tiered_summary = report['tiered_summary']
        
        summary_text = f"""
SCOPUS TIERED DATA QUALITY HEALTH CHECK REPORT
{'='*70}
Generated: {metadata['generated_at']}
Datasource: {metadata['datasource']}
Staging Table: {metadata['staging_table']}
Load ID: {lineage['load_id'] or 'Not found'}
Database: {metadata['database_file']}
Table Rows: {lineage['table_row_count']:,}

TIERED VALIDATION STATUS:
{'='*40}
Overall Status: {tiered_summary['overall_status']}
Critical Status: {tiered_summary['critical_status']}
Quality Status: {tiered_summary['quality_status']}
Monitoring Warnings: {tiered_summary['monitoring_warnings']}
Pipeline Blocking: {'YES' if tiered_summary['pipeline_blocking'] else 'NO'}

SUMMARY METRICS:
{'='*30}
Total Checks: {summary['total_checks']}
Passed: {summary['passed_checks']}
Failed: {summary['failed_checks']}
Errors: {summary['error_checks']}
Warnings: {summary['warning_checks']}
Pass Rate: {summary['pass_rate_percentage']}%
Tiers Executed: {summary['tiers_executed']}/3

BREAKDOWN BY TIER:
{'='*30}
Critical: {report['passed_checks_summary']['by_tier']['critical']} passed
Quality: {report['passed_checks_summary']['by_tier']['quality']} passed
Monitoring: {report['passed_checks_summary']['by_tier']['monitoring']} passed

"""
        
        # Add critical failures if any
        critical_failures = [c for c in report['failed_checks'] + report['error_checks'] if c['tier'] == 'critical']
        if critical_failures:
            summary_text += f"""
CRITICAL FAILURES (PIPELINE BLOCKING):
{'='*50}
"""
            for check in critical_failures:
                summary_text += f"• {check['name']} [{check['outcome'].upper()}]\n"
                if check.get('check_value'):
                    summary_text += f"  Actual Value: {check['check_value']}\n"
        
        # Add quality failures if any
        quality_failures = [c for c in report['failed_checks'] + report['error_checks'] if c['tier'] == 'quality']
        if quality_failures:
            summary_text += f"""
QUALITY FAILURES (INVESTIGATION NEEDED):
{'='*50}
"""
            for check in quality_failures[:5]:  # Show first 5
                summary_text += f"• {check['name']} [{check['outcome'].upper()}]\n"
            if len(quality_failures) > 5:
                summary_text += f"... and {len(quality_failures) - 5} more quality issues\n"
        
        # Add monitoring warnings if any
        if report['warning_checks']:
            summary_text += f"""
MONITORING WARNINGS (TREND ANALYSIS):
{'='*50}
"""
            for check in report['warning_checks'][:3]:  # Show first 3
                summary_text += f"• {check['name']}\n"
            if len(report['warning_checks']) > 3:
                summary_text += f"... and {len(report['warning_checks']) - 3} more warnings\n"
        
        # Add recommendation based on status
        summary_text += f"""
RECOMMENDATIONS:
{'='*30}
"""
        if tiered_summary['overall_status'] == 'CRITICAL_FAIL':
            summary_text += "IMMEDIATE ACTION REQUIRED: Fix critical issues before proceeding\n"
        elif tiered_summary['overall_status'] == 'QUALITY_FAIL':
            summary_text += "INVESTIGATION NEEDED: Address quality issues when possible\n"
        elif tiered_summary['overall_status'] == 'WARNING':
            summary_text += "MONITORING: Track trends in warning indicators\n"
        else:
            summary_text += "EXCELLENT: All validation checks passed\n"
        
        return summary_text


def run_tiered_health_check(config_file: str = "configuration.yml", 
                           checks_dir: str = "checks", 
                           staging_table: str = None,
                           db_file_path: str = "scopus_test.db",
                           output_dir: str = "./dq_reports",
                           datasource_name: str = "scopus_duckdb",
                           print_summary: bool = True) -> Tuple[str, str]:
    """
    Main function to run tiered health check and save JSON report
    
    Args:
        config_file: Path to Soda configuration file
        checks_dir: Directory containing tiered check files
        staging_table: Name of the staging table to audit
        db_file_path: Path to DuckDB database file
        output_dir: Directory to save reports
        datasource_name: Name of datasource in config
        print_summary: Whether to print summary to console
        
    Returns:
        Tuple of (overall_status, report_file_path)
    """
    if not staging_table:
        raise ValueError("staging_table is required for tiered health check")
    
    try:
        # Initialise tiered reporter
        reporter = TieredSodaHealthCheckReporter(
            config_file=config_file, 
            checks_dir=checks_dir, 
            staging_table=staging_table,
            db_file_path=db_file_path,
            datasource_name=datasource_name
        )
        
        # Run tiered scan (sequential execution)
        overall_status, combined_results = reporter.run_tiered_scan()
        
        # Create comprehensive report
        health_report = reporter.create_tiered_health_check_report(overall_status, combined_results)
        
        # Save JSON report
        report_file_path = reporter.save_report_json(health_report, output_dir)
        
        # Print summary if requested
        if print_summary:
            summary = reporter.generate_tiered_summary_report(health_report)
            print(summary)
        
        return overall_status, report_file_path
        
    except Exception as e:
        logger.error(f"Tiered health check failed: {e}")
        raise


if __name__ == "__main__":
    """
    Command line execution for tiered validation
    Usage: python tiered_soda_dq.py <staging_table> [config_file] [checks_dir] [db_file_path] [output_dir]
    """
    
    if len(sys.argv) < 2:
        print("Usage: python soda_dq.py <staging_table> [config_file] [checks_dir] [db_file_path] [output_dir]")
        print("Example: python soda_dq.py silver_stg_scopus")
        print("Example: python soda_dq.py silver_stg_scopus configuration.yml checks scopus_test.db dq_reports/")
        sys.exit(1)
    
    staging_table = sys.argv[1]
    config_file = sys.argv[2] if len(sys.argv) > 2 else "configuration.yml"
    checks_dir = sys.argv[3] if len(sys.argv) > 3 else "checks"
    db_file_path = sys.argv[4] if len(sys.argv) > 4 else "scopus_test.db"
    output_dir = sys.argv[5] if len(sys.argv) > 5 else "./dq_reports"
    
    try:
        overall_status, report_path = run_tiered_health_check(
            config_file=config_file,
            checks_dir=checks_dir,
            staging_table=staging_table,
            db_file_path=db_file_path,
            output_dir=output_dir
        )
        
        print(f"\nTiered health check completed!")
        print(f"Scanned staging table: {staging_table}")
        print(f"Overall Status: {overall_status}")
        print(f"Report saved to: {report_path}")
        
        # Exit with appropriate code based on status
        if overall_status == "CRITICAL_FAIL":
            sys.exit(2)  # Critical failure
        elif overall_status == "QUALITY_FAIL":
            sys.exit(1)  # Quality issues
        else:
            sys.exit(0)  # Success or warnings only
        
    except Exception as e:
        print(f"Tiered health check failed: {e}")
        sys.exit(3)  # System error