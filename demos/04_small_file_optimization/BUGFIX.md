# Small File Optimization Demo Bug Fix

## Issue Description

The small file optimization demo was reporting incorrect storage sizes and improvements, showing DuckLake using more storage than traditional formats, which contradicted the expected behavior of the optimization feature.

### Symptoms

1. Storage size discrepancies between different sections of the output
2. Negative improvement percentages (suggesting degradation rather than improvement)
3. Inconsistent file counting between traditional and DuckLake formats

## Root Cause Analysis

### 1. Storage Size Calculation

The original code had several issues in how it calculated storage sizes:

```python
# Original problematic code
catalog_size = os.path.getsize(catalog_path.replace("ducklake:", ""))
data_size = get_directory_size(data_files_path)
total_size = catalog_size + data_size
```

Problems:

- Did not properly handle symbolic links
- Potential double-counting of files
- Inconsistent measurement methods between traditional and DuckLake formats
- No validation of file existence before size calculation

### 2. Improvement Calculation

The improvement percentage calculation was scattered throughout the code and sometimes inverted:

```python
# Original problematic code
size_reduction = ((r["trad_size"] - r["duck_size"]) / r["trad_size"]) * 100
```

Problems:

- No handling of edge cases (e.g., zero sizes)
- Inconsistent calculation across different metrics
- Results were not properly validated

## Fix Implementation

### 1. Centralized Storage Size Calculation

Added a dedicated function for calculating DuckLake storage size:

```python
def get_ducklake_size(catalog_path):
    """Calculate the total size of a DuckLake catalog including data files."""
    base_path = catalog_path.replace("ducklake:", "")
    total_size = 0
    
    # Add catalog file size
    if os.path.exists(base_path):
        total_size += os.path.getsize(base_path)
    
    # Add data files size
    data_files_path = base_path + ".files"
    if os.path.exists(data_files_path):
        for root, _, files in os.walk(data_files_path):
            for f in files:
                if f.endswith('.parquet'):
                    file_path = os.path.join(root, f)
                    if os.path.exists(file_path) and not os.path.islink(file_path):
                        total_size += os.path.getsize(file_path)
    
    return total_size
```

Improvements:

- Handles symbolic links correctly
- Validates file existence
- Only counts relevant files (.parquet)
- Consistent measurement method

### 2. Standardized Improvement Calculation

Added a dedicated function for calculating improvements:

```python
def calculate_improvement(traditional_size, ducklake_size):
    """Calculate the percentage improvement (reduction) in size."""
    if traditional_size == 0:
        return 0
    return ((traditional_size - ducklake_size) / traditional_size) * 100
```

Improvements:

- Handles edge cases
- Consistent calculation method
- Clear, reusable function

### 3. Enhanced Results Reporting

Updated the performance comparison to use the new functions:

```python
# Calculate improvements
file_improvement = calculate_improvement(trad_files, duck_files)
size_improvement = calculate_improvement(trad_size, duck_size)

results.append({
    'updates': num_updates,
    'trad_time': trad_time,
    'trad_files': trad_files,
    'trad_size': trad_size,
    'duck_time': duck_time,
    'duck_files': duck_files,
    'duck_size': duck_size,
    'file_improvement': file_improvement,
    'size_improvement': size_improvement
})
```

## Expected Results

After the fix:

1. Storage size calculations are consistent across all sections
2. Improvement percentages correctly show reduction in storage and file count
3. More accurate representation of DuckLake's small file optimization benefits

## Testing

To verify the fix:

1. Run the demo script: `python demos/04_small_file_optimization/demo.py`
2. Verify that:
   - Storage sizes are consistent
   - Improvement percentages are positive (showing actual improvements)
   - File counts match expected behavior
   - No symbolic link-related errors occur
