#!/usr/bin/env python3
"""Zeppelin to Databricks Notebook Converter - Minimal Version"""

import argparse, glob, json, os, sys
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from collections import Counter

# Configuration
INTERPRETER_MAPPINGS = {
    '%pyspark': '%python', '%sh': '%sh', '%spark': '%scala', '%sql': '%sql', '%md': '%md', '%r': '%r',
    '%spark.sql': '%sql', '%spark.pyspark': '%python', '%spark.ipyspark': '%python', 
    '%spark.r': '%r', '%spark.ir': '%r', '%spark.conf': '%scala',
    '%python.ipython': '%python', '%python': '%python',
    '%r.ir': '%r', '%r.r': '%r', '%r.shiny': '%r', '%file': '%sh',
    'scala': '%scala', 'python': '%python', 'sql': '%sql',
}

FILE_EXTENSIONS = {'%python': '.py', '%scala': '.scala', '%sql': '.sql', '%r': '.r', '%md': '.md', '%sh': '.sh'}
COMMENT_STYLES = {'%python': '#', '%r': '#', '%sh': '#', '%scala': '//', '%md': '//', '%sql': '--'}
ANONYMOUS_USERS = ['unknown', 'anonymous']

class UnsupportedLanguageError(Exception): pass

@dataclass
class ProcessingResult:
    successful_files: List[str]
    failed_files: List[Tuple[str, str]]
    skipped_files: List[str]
    interpreter_stats: Counter

def load_notebook_json(file_path: str) -> Dict:
    """Load Zeppelin notebook JSON file"""
    with open(file_path, encoding='utf-8-sig') as f:
        data = json.load(f)
    if 'paragraphs' not in data:
        raise ValueError(f"Missing 'paragraphs' field in {file_path}")
    
    # Find latest date from paragraphs
    latest_date = None
    for p in data['paragraphs']:
        if 'dateUpdated' in p and p['dateUpdated']:
            try:
                date = datetime.strptime(p['dateUpdated'], "%Y-%m-%d %H:%M:%S.%f")
                if latest_date is None or date > latest_date:
                    latest_date = date
            except ValueError:
                pass
    
    return {
        'json': data['paragraphs'],
        'name': data.get('name', ''),
        'lang': data.get('config', {}).get('defaultLang', ''),
        'last_updated': latest_date
    }

def convert_notebook(notebook_json: List[Dict], default_language: str) -> Tuple[List[str], Counter]:
    """Convert Zeppelin notebook to Databricks format"""
    content_lines = []
    comment = COMMENT_STYLES[default_language]
    prefix = f"{comment} MAGIC"
    interpreter_usage = Counter()
    
    content_lines.append(f"{comment} Databricks notebook source\n")
    for paragraph in notebook_json:
        cell_text = paragraph.get('text', '').strip()
        if not cell_text:
            continue

        content_lines.append(f"{comment} DBTITLE 1, {paragraph.get('title')}\n")

        lines = cell_text.split('\n')
        interpreter_type = default_language
        cell_lines = lines[:]
        interpreter_comment = ''

        # Check for interpreter directive
        if lines and lines[0].strip().startswith('%'):
            first_line = lines[0].strip()
            potential_interpreter = first_line.split(' ')[0] if ' ' in first_line else first_line
            if potential_interpreter in INTERPRETER_MAPPINGS:
                interpreter_type = INTERPRETER_MAPPINGS[potential_interpreter]
                if interpreter_type != potential_interpreter:
                    interpreter_comment = f"{prefix} {COMMENT_STYLES[interpreter_type]} NOTE: converted from {potential_interpreter} interpreter from Zeppelin to {interpreter_type} for Databricks. Also note that inline interpreters are not supported in Databricks and will need to be converted manually.\n"
            else:
                interpreter_type = potential_interpreter
                interpreter_comment = f"{prefix} {COMMENT_STYLES[default_language]} NOTE: unrecognized {potential_interpreter} interpreter from Zeppelin. Also note that inline interpreters are not supported in Databricks and will need to be converted manually.\n"
                
            cell_lines = lines[1:]
        
        # Track interpreter usage
        interpreter_usage[interpreter_type] += 1

        content_lines.append(f"{prefix} {interpreter_type}\n{interpreter_comment}")
            
        # Process cell content
        for line in cell_lines:
            line_to_add = f"{prefix} {line}\n"
            content_lines.append(line_to_add)
        # Add cell separator
        content_lines.append(f"{comment} COMMAND ----------\n\n")
    
    return content_lines, interpreter_usage

def find_notebook_files(directory: str, ignore_dirs: List[str] = None) -> List[str]:
    """Find notebook files in directory"""
    ignore_dirs = ignore_dirs or []
    files = []
    for pattern in ['**/*.json', '**/*.zpln']:
        for file_path in glob.glob(os.path.join(directory, pattern), recursive=True):
            rel_path = os.path.relpath(file_path, directory)
            if not any(part in ignore_dirs for part in rel_path.split(os.sep)):
                files.append(file_path)
    return sorted(set(files))

def resolve_filename_conflicts(output_path: str) -> str:
    """Resolve conflicts when filename matches existing directory name"""
    if not os.path.exists(output_path):
        return output_path
    
    # If path exists and is a directory, we have a conflict
    if os.path.isdir(output_path):
        # Extract directory, base name, and extension
        directory = os.path.dirname(output_path)
        filename = os.path.basename(output_path)
        name, ext = os.path.splitext(filename)
        
        # Try different naming strategies
        conflict_resolved = False
        counter = 1
        
        # Strategy 1: Add "_notebook" suffix
        new_filename = f"{name}_notebook{ext}"
        new_path = os.path.join(directory, new_filename)
        if not os.path.exists(new_path):
            return new_path
        
        # Strategy 2: Add "_file" suffix
        new_filename = f"{name}_file{ext}"
        new_path = os.path.join(directory, new_filename)
        if not os.path.exists(new_path):
            return new_path
        
        # Strategy 3: Add incremental numbers
        while not conflict_resolved and counter <= 999:
            new_filename = f"{name}_{counter:03d}{ext}"
            new_path = os.path.join(directory, new_filename)
            if not os.path.exists(new_path):
                return new_path
            counter += 1
        
        # Fallback: Use timestamp if all else fails
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_filename = f"{name}_{timestamp}{ext}"
        new_path = os.path.join(directory, new_filename)
        return new_path
    
    # If it's a file conflict, try incremental naming
    elif os.path.isfile(output_path):
        directory = os.path.dirname(output_path)
        filename = os.path.basename(output_path)
        name, ext = os.path.splitext(filename)
        
        counter = 1
        while counter <= 999:
            new_filename = f"{name}_{counter:03d}{ext}"
            new_path = os.path.join(directory, new_filename)
            if not os.path.exists(new_path):
                return new_path
            counter += 1
        
        # Fallback with timestamp
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_filename = f"{name}_{timestamp}{ext}"
        new_path = os.path.join(directory, new_filename)
        return new_path
    
    return output_path

def generate_output_path(input_file: str, output_dir: str, source_dir: str, language: str, name: str) -> str:
    """Generate flat output file path with folder path encoded in filename"""
    extension = FILE_EXTENSIONS.get(language, '.py')
    
    # Get the relative directory path to encode in filename
    folder_path = ""
    if source_dir:
        rel_dir = os.path.dirname(os.path.relpath(input_file, source_dir))
        if rel_dir and rel_dir != '.':
            # Replace path separators with underscores and sanitize
            folder_path = rel_dir.replace(os.sep, '_').replace('/', '_').replace('\\', '_')
            folder_path = sanitize_path_component(folder_path)
            folder_path += '_'
    else:
        # For single files, still encode the parent directory name if it exists
        parent_dir = os.path.basename(os.path.dirname(input_file))
        if parent_dir and parent_dir not in ['.', '..', os.path.basename(os.getcwd())]:
            folder_path = sanitize_path_component(parent_dir) + '_'
    
    # Sanitize the notebook name
    if name:
        safe_name = sanitize_path_component(name)
        if not safe_name or safe_name in ['.', '..']:
            safe_name = 'unnamed_notebook'
    else:
        safe_name = 'unnamed_notebook'
    
    # Combine folder path and filename
    final_filename = folder_path + safe_name
    
    # Ensure reasonable length (filesystem limits)
    if len(final_filename) > 200:
        # Truncate but preserve some of the original name
        truncated_folder = folder_path[:100] if len(folder_path) > 100 else folder_path
        remaining_length = 200 - len(truncated_folder) - len(extension)
        if remaining_length > 10:  # Keep at least 10 chars of original name
            safe_name = safe_name[:remaining_length]
        final_filename = truncated_folder + safe_name
    
    if not final_filename.endswith(extension):
        final_filename += extension
    
    # Generate output path (always flat - just output_dir + filename)
    if output_dir:
        output_dir = os.path.abspath(os.path.expanduser(output_dir))
        output_path = os.path.join(output_dir, final_filename)
    else:
        output_path = os.path.join(os.path.dirname(input_file), final_filename)
    
    # Resolve filename conflicts (now only file conflicts, no directory conflicts)
    output_path = resolve_filename_conflicts(output_path)
    
    return os.path.normpath(output_path)

def sanitize_path_component(component: str) -> str:
    """Sanitize a path component for use in filename"""
    if not component:
        return ""
    
    # Replace ALL problematic characters for FUSE/network filesystems
    problematic_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '\t', '\n', '\r']
    safe_component = component
    for char in problematic_chars:
        safe_component = safe_component.replace(char, '_')
    
    # Remove any remaining control characters
    safe_component = ''.join(c if ord(c) >= 32 and c not in problematic_chars else '_' for c in safe_component)
    safe_component = ' '.join(safe_component.split())  # Normalize whitespace
    safe_component = safe_component.strip('. ')  # Remove problematic leading/trailing chars
    
    return safe_component

def process_single_file(file_path: str, default_language: str, output_dir: str = None, source_dir: str = None, skip_old_days: int = None) -> Tuple[bool, str, Counter]:
    """Process a single notebook file"""
    empty_stats = Counter()
    try:
        notebook_data = load_notebook_json(file_path)
        
        # Check if all paragraphs are empty
        paragraphs = notebook_data['json']
        has_content = False
        for paragraph in paragraphs:
            cell_text = paragraph.get('text', '').strip()
            if cell_text:
                has_content = True
                break
        
        if not has_content:
            return False, "Skipped (all paragraphs are empty)", empty_stats
        
        # Check age if specified
        if skip_old_days and notebook_data['last_updated']:
            cutoff_date = datetime.now() - timedelta(days=skip_old_days)
            if notebook_data['last_updated'] < cutoff_date:
                return False, f"Skipped (older than {skip_old_days} days)", empty_stats
        
        # Convert notebook
        content_lines, interpreter_stats = convert_notebook(notebook_data['json'], default_language)
        
        # Generate output path (with conflict resolution if needed)
        original_name = notebook_data['name']
        output_path = generate_output_path(file_path, output_dir, source_dir, default_language, original_name)
        
        # Check if filename was changed due to conflict resolution
        conflict_resolved = False
        if original_name:
            # Sanitize the original name the same way as in generate_output_path
            safe_original = original_name
            problematic_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '\t', '\n', '\r']
            for char in problematic_chars:
                safe_original = safe_original.replace(char, '_')
            safe_original = ''.join(c if ord(c) >= 32 and c not in problematic_chars else '_' for c in safe_original)
            safe_original = ' '.join(safe_original.split())
            safe_original = safe_original.strip('. ')
            if not safe_original:
                safe_original = 'unnamed_notebook'
            
            expected_filename = safe_original + FILE_EXTENSIONS.get(default_language, '.py')
            actual_filename = os.path.basename(output_path)
            if actual_filename != expected_filename:
                conflict_resolved = True
        
        # Create output directory (flat structure - only the main output dir)
        output_dir_only = os.path.dirname(output_path)
        if output_dir_only:
            os.makedirs(output_dir_only, exist_ok=True)
        
        # Remove existing file if it exists for proper overwriting
        if os.path.exists(output_path):
            try:
                os.remove(output_path)
            except (OSError, PermissionError):
                pass  # Continue if we can't remove, try overwriting
        
        # Write with explicit newline handling and flushing for FUSE
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            for line in content_lines:
                f.write(line)
            f.flush()
            os.fsync(f.fileno())  # Force write to disk for FUSE
        
        success_message = f"Successfully converted to {output_path}"
        if conflict_resolved:
            success_message += " (filename conflict resolved)"
        return True, success_message, interpreter_stats
    except Exception as e:
        return False, str(e), empty_stats

def process_files(input_files: List[str], default_language: str, output_dir: str = None, source_dir: str = None, skip_old_days: int = None) -> ProcessingResult:
    """Process multiple files"""
    successful, failed, skipped = [], [], []
    total_interpreter_stats = Counter()
    
    for file_path in input_files:
        success, message, interpreter_stats = process_single_file(file_path, default_language, output_dir, source_dir, skip_old_days)
        if success:
            successful.append(file_path)
            total_interpreter_stats.update(interpreter_stats)
            print(f"  ✓ {file_path}: {message}")
        elif "Skipped" in message:
            skipped.append(file_path)
            print(f"  ⊘ {file_path}: {message}")
        else:
            failed.append((file_path, message))
            print(f"  ✗ {file_path}: {message}")
    
    return ProcessingResult(successful, failed, skipped, total_interpreter_stats)

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Convert Zeppelin notebooks to Databricks format')
    
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('--file', help='Single notebook file (.json or .zpln)')
    input_group.add_argument('--directory', help='Directory containing notebook files')
    
    parser.add_argument('--language', default='spark', choices=['spark', 'python', 'pyspark', 'sql', 'r', 'scala'], help='Default language (default: spark)')
    parser.add_argument('--out_dir', help='Output directory (default: same as input)')
    parser.add_argument('--skip_old_days', type=int, help='Skip notebooks older than N days')
    parser.add_argument('--ignore_dirs', nargs='+', default=[], help='Directory names to ignore')
    parser.add_argument('--show-interpreters', action='store_true', help='Display interpreter usage statistics')
    
    args = parser.parse_args()
    
    # Map language to interpreter
    language_mapping = {'spark': '%scala', 'scala': '%scala', 'python': '%python', 'pyspark': '%python', 'sql': '%sql', 'r': '%r'}
    default_language = language_mapping.get(args.language.lower())
    if not default_language:
        print(f"Error: Unsupported language: {args.language}")
        sys.exit(1)
    
    # Get input files
    if args.file:
        if not os.path.exists(args.file):
            print(f"Error: File not found: {args.file}")
            sys.exit(1)
        input_files = [args.file]
        source_dir = None
    else:
        if not os.path.exists(args.directory):
            print(f"Error: Directory not found: {args.directory}")
            sys.exit(1)
        input_files = find_notebook_files(args.directory, args.ignore_dirs)
        source_dir = args.directory
        if not input_files:
            print(f"No notebook files found in: {args.directory}")
            sys.exit(0)
    
    print(f"Found {len(input_files)} notebook file(s) to process")
    print(f"Default language: {args.language} -> {default_language}")
    if args.out_dir:
        print(f"Output directory: {args.out_dir}")
    if args.skip_old_days:
        print(f"Skipping files older than {args.skip_old_days} days")
    print()
    
    # Process files
    result = process_files(input_files, default_language, args.out_dir, source_dir, args.skip_old_days)
    
    # Print summary
    total = len(result.successful_files) + len(result.failed_files) + len(result.skipped_files)
    print(f"\nSUMMARY: {len(result.successful_files)}/{total} successful, {len(result.failed_files)} failed, {len(result.skipped_files)} skipped")
    
    # Print interpreter usage statistics if requested
    if args.show_interpreters and result.interpreter_stats:
        print("\nINTERPRETER USAGE:")
        total_cells = sum(result.interpreter_stats.values())
        for interpreter, count in result.interpreter_stats.most_common():
            percentage = (count / total_cells) * 100 if total_cells > 0 else 0
            print(f"  {interpreter}: {count} cells ({percentage:.1f}%)")
        print(f"  Total cells: {total_cells}")
    
    if result.failed_files:
        print("\nFailed files:")
        for file_path, error in result.failed_files:
            print(f"  ✗ {file_path}: {error}")
    
    sys.exit(1 if result.failed_files else 0)

if __name__ == '__main__':
    main()
