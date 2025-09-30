"""
Great Expectations based data profiling and expectation suggestion tool
For quickly creating validation cases for a given table
"""

import great_expectations as gx
from pathlib import Path
import warnings
import json
import duckdb

warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL 1.1.1+")

def analyse_data_directly():
    """Analyse data directly using DuckDB for comprehensive profiling"""
    
    try:
        print("Running Direct Data Analysis...")
        print("="*50)
        
        conn = duckdb.connect("scopus_test.db")
        
        # Get column information
        columns_info = conn.execute("PRAGMA table_info(silver_stg_scopus)").fetchall()
        column_names = [col[1] for col in columns_info]
        column_types = {col[1]: col[2] for col in columns_info}
        
        print(f"Analysing {len(column_names)} columns...")
        
        analysis_results = {}
        
        for column in column_names:
            print(f"\nAnalysing: {column}")
            
            try:
                column_analysis = {}
                column_type = column_types[column]
                column_analysis['data_type'] = column_type
                
                # Basic stats for all columns
                total_rows = conn.execute("SELECT COUNT(*) FROM silver_stg_scopus").fetchone()[0]
                null_count = conn.execute(f"SELECT COUNT(*) FROM silver_stg_scopus WHERE {column} IS NULL").fetchone()[0]
                non_null_count = total_rows - null_count
                
                column_analysis['total_rows'] = total_rows
                column_analysis['null_count'] = null_count
                column_analysis['non_null_count'] = non_null_count
                column_analysis['null_percentage'] = (null_count / total_rows) * 100 if total_rows > 0 else 0
                column_analysis['completeness'] = (non_null_count / total_rows) * 100 if total_rows > 0 else 0
                
                # Unique values
                if non_null_count > 0:
                    unique_count = conn.execute(f"SELECT COUNT(DISTINCT {column}) FROM silver_stg_scopus WHERE {column} IS NOT NULL").fetchone()[0]
                    column_analysis['unique_count'] = unique_count
                    column_analysis['uniqueness_percentage'] = (unique_count / non_null_count) * 100
                else:
                    column_analysis['unique_count'] = 0
                    column_analysis['uniqueness_percentage'] = 0
                
                # Type-specific analysis
                if 'INT' in column_type.upper() or 'DOUBLE' in column_type.upper() or 'FLOAT' in column_type.upper():
                    # Numeric analysis
                    if non_null_count > 0:
                        stats = conn.execute(f"""
                            SELECT 
                                MIN({column}) as min_val,
                                MAX({column}) as max_val,
                                AVG({column}) as avg_val,
                                COUNT(DISTINCT {column}) as distinct_count
                            FROM silver_stg_scopus 
                            WHERE {column} IS NOT NULL
                        """).fetchone()
                        
                        column_analysis['min_value'] = stats[0]
                        column_analysis['max_value'] = stats[1]
                        column_analysis['avg_value'] = round(stats[2], 2) if stats[2] else None
                        column_analysis['range'] = stats[1] - stats[0] if stats[0] is not None and stats[1] is not None else None
                
                elif 'VARCHAR' in column_type.upper() or 'TEXT' in column_type.upper():
                    # String analysis
                    if non_null_count > 0:
                        length_stats = conn.execute(f"""
                            SELECT 
                                MIN(LENGTH({column})) as min_length,
                                MAX(LENGTH({column})) as max_length,
                                AVG(LENGTH({column})) as avg_length
                            FROM silver_stg_scopus 
                            WHERE {column} IS NOT NULL AND {column} != ''
                        """).fetchone()
                        
                        column_analysis['min_length'] = length_stats[0]
                        column_analysis['max_length'] = length_stats[1]
                        column_analysis['avg_length'] = round(length_stats[2], 1) if length_stats[2] else None
                        
                        # Sample values
                        sample_values = conn.execute(f"""
                            SELECT DISTINCT {column} 
                            FROM silver_stg_scopus 
                            WHERE {column} IS NOT NULL 
                            LIMIT 5
                        """).fetchall()
                        column_analysis['sample_values'] = [val[0] for val in sample_values]
                
                # Check for potential categorical data
                if column_analysis['unique_count'] <= 10 and non_null_count > 0:
                    value_counts = conn.execute(f"""
                        SELECT {column}, COUNT(*) as count
                        FROM silver_stg_scopus 
                        WHERE {column} IS NOT NULL
                        GROUP BY {column}
                        ORDER BY count DESC
                    """).fetchall()
                    column_analysis['value_distribution'] = dict(value_counts)
                
                analysis_results[column] = column_analysis
                
                # Print summary
                print(f"   Type: {column_type}")
                print(f"   Completeness: {column_analysis['completeness']:.1f}%")
                print(f"   Uniqueness: {column_analysis['uniqueness_percentage']:.1f}%")
                
                if 'min_value' in column_analysis:
                    print(f"   Range: {column_analysis['min_value']} to {column_analysis['max_value']}")
                if 'min_length' in column_analysis:
                    print(f"   Length: {column_analysis['min_length']} to {column_analysis['max_length']} chars")
                if 'value_distribution' in column_analysis:
                    print(f"   Categories: {list(column_analysis['value_distribution'].keys())}")
                
            except Exception as e:
                print(f"   Analysis failed: {str(e)[:50]}...")
                analysis_results[column] = {'error': str(e)}
        
        conn.close()
        
        # Save detailed analysis
        analysis_file = Path("detailed_data_analysis.json")
        with open(analysis_file, 'w') as f:
            json.dump(analysis_results, f, indent=2, default=str)
        
        print(f"\nDetailed analysis saved to: {analysis_file}")
        return analysis_results
        
    except Exception as e:
        print(f"Direct analysis failed: {e}")
        return None

def generate_expectation_suggestions(analysis_results):
    """Generate intelligent expectation suggestions based on analysis"""
    
    if not analysis_results:
        return []
    
    print("\nGenerating Expectation Suggestions...")
    print("="*50)
    
    suggestions = []
    
    for column, analysis in analysis_results.items():
        if 'error' in analysis:
            continue
        
        print(f"\nSuggestions for: {column}")
        
        # 1. Null/Completeness expectations
        null_pct = analysis.get('null_percentage', 0)
        if null_pct == 0:
            suggestion = f"expect_column_values_to_not_be_null('{column}')"
            suggestions.append({'column': column, 'expectation': suggestion, 'reason': 'No null values found'})
            print(f"   {suggestion}")
            print(f"      Reason: No null values found")
        elif null_pct < 5:
            suggestion = f"expect_column_values_to_not_be_null('{column}', mostly=0.95)"
            suggestions.append({'column': column, 'expectation': suggestion, 'reason': f'{null_pct:.1f}% null values'})
            print(f"   {suggestion}")
            print(f"      Reason: {null_pct:.1f}% null values")
        
        # 2. Uniqueness expectations
        uniqueness_pct = analysis.get('uniqueness_percentage', 0)
        if uniqueness_pct == 100:
            suggestion = f"expect_column_values_to_be_unique('{column}')"
            suggestions.append({'column': column, 'expectation': suggestion, 'reason': 'All values are unique'})
            print(f"   {suggestion}")
            print(f"      Reason: All values are unique")
        elif uniqueness_pct > 95:
            suggestion = f"expect_column_values_to_be_unique('{column}', mostly=0.95)"
            suggestions.append({'column': column, 'expectation': suggestion, 'reason': f'{uniqueness_pct:.1f}% unique values'})
            print(f"   {suggestion}")
            print(f"      Reason: {uniqueness_pct:.1f}% unique values")
        
        # 3. Type expectations
        data_type = analysis.get('data_type', '')
        if data_type:
            suggestion = f"expect_column_values_to_be_of_type('{column}', '{data_type}')"
            suggestions.append({'column': column, 'expectation': suggestion, 'reason': f'Column type is {data_type}'})
            print(f"   {suggestion}")
            print(f"      Reason: Column type is {data_type}")
        
        # 4. Range expectations for numeric columns
        if 'min_value' in analysis and 'max_value' in analysis:
            min_val = analysis['min_value']
            max_val = analysis['max_value']
            if min_val is not None and max_val is not None:
                # Add 10% buffer to the range
                range_buffer = abs(max_val - min_val) * 0.1 if max_val != min_val else 1
                suggestion = f"expect_column_values_to_be_between('{column}', min_value={min_val - range_buffer:.0f}, max_value={max_val + range_buffer:.0f})"
                suggestions.append({'column': column, 'expectation': suggestion, 'reason': f'Observed range: {min_val} to {max_val} (10% buffer added)'})
                print(f"   {suggestion}")
                print(f"      Reason: Observed range: {min_val} to {max_val} (10% buffer added)")
        
        # 5. Length expectations for string columns
        if 'min_length' in analysis and 'max_length' in analysis:
            min_len = analysis['min_length']
            max_len = analysis['max_length']
            if min_len is not None and max_len is not None and min_len > 0:
                suggestion = f"expect_column_value_lengths_to_be_between('{column}', min_value={min_len}, max_value={max_len + 5})"
                suggestions.append({'column': column, 'expectation': suggestion, 'reason': f'Observed length range: {min_len} to {max_len} chars (buffer added)'})
                print(f"   {suggestion}")
                print(f"      Reason: Observed length range: {min_len} to {max_len} chars (buffer added)")
        
        # 6. Categorical value expectations
        if 'value_distribution' in analysis:
            values = list(analysis['value_distribution'].keys())
            if len(values) <= 10:  # Only for small sets
                values_str = str(values).replace("'", '"')
                suggestion = f"expect_column_values_to_be_in_set('{column}', {values_str})"
                suggestions.append({'column': column, 'expectation': suggestion, 'reason': f'Low cardinality field with {len(values)} distinct values'})
                print(f"   {suggestion}")
                print(f"      Reason: Low cardinality field with {len(values)} distinct values")
        
        # 7. Special Scopus-specific expectations
        if column == 'eid':
            suggestion = f"expect_column_values_to_match_regex('{column}', r'^2-s2\\.0-[0-9]+$')"
            suggestions.append({'column': column, 'expectation': suggestion, 'reason': 'Scopus EID format validation'})
            print(f"   {suggestion}")
            print(f"      Reason: Scopus EID format validation")
        
        elif column == 'doi' and analysis.get('completeness', 0) > 50:
            suggestion = f"expect_column_values_to_match_regex('{column}', r'^10\\.[0-9]+/.*', mostly=0.8)"
            suggestions.append({'column': column, 'expectation': suggestion, 'reason': 'DOI format validation'})
            print(f"   {suggestion}")
            print(f"      Reason: DOI format validation")
        
        elif column == 'citations' and 'min_value' in analysis:
            suggestion = f"expect_column_values_to_be_between('{column}', min_value=0, max_value=100000)"
            suggestions.append({'column': column, 'expectation': suggestion, 'reason': 'Reasonable citation count range'})
            print(f"   {suggestion}")
            print(f"      Reason: Reasonable citation count range")
    
    # Save suggestions to file
    suggestions_file = Path("expectation_suggestions.json")
    with open(suggestions_file, 'w') as f:
        json.dump(suggestions, f, indent=2, default=str)
    
    print(f"\n{'='*50}")
    print(f"Generated {len(suggestions)} expectation suggestions")
    print(f"Suggestions saved to: {suggestions_file}")
    
    return suggestions

if __name__ == "__main__":
    print("Comprehensive Scopus Data Profiling & Expectation Suggestions")
    print("="*70)
    
    if not Path("scopus_test.db").exists():
        print("Database not found")
        exit(1)
    
    # Step 1: Direct data analysis
    print("\nStep 1: Comprehensive Data Analysis")
    analysis_results = analyse_data_directly()
    
    if analysis_results:
        # Step 2: Generate expectation suggestions
        print("\nStep 2: Expectation Suggestion Generation")
        suggestions = generate_expectation_suggestions(analysis_results)
        
        if suggestions:
            print(f"\nAnalysis Complete!")
            print(f"Analysed {len(analysis_results)} columns")
            print(f"Generated {len(suggestions)} expectation suggestions")
            print(f"\nOutput files:")
            print(f"1. detailed_data_analysis.json - Full data profiling results")
            print(f"2. expectation_suggestions.json - Suggested validations per column")
        else:
            print("No suggestions generated")
    else:
        print("Data analysis failed")
