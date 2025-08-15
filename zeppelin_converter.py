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

def generate_output_path(input_file: str, output_dir: str, source_dir: str, language: str, name: str) -> str:
    """Generate output file path with FUSE-compatible sanitization"""
    extension = FILE_EXTENSIONS.get(language, '.py')
    
    # Comprehensive filename sanitization for FUSE compatibility
    if name:
        safe_name = name
        # Replace ALL problematic characters for FUSE/network filesystems
        problematic_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '\t', '\n', '\r']
        for char in problematic_chars:
            safe_name = safe_name.replace(char, '_')
        # Remove any remaining control characters
        safe_name = ''.join(c if ord(c) >= 32 and c not in problematic_chars else '_' for c in safe_name)
        safe_name = ' '.join(safe_name.split())  # Normalize whitespace
        safe_name = safe_name.strip('. ')  # Remove problematic leading/trailing chars
        if len(safe_name) > 200:
            safe_name = safe_name[:200].strip('. ')
        if not safe_name or safe_name in ['.', '..']:
            safe_name = 'unnamed_notebook'
    else:
        safe_name = 'unnamed_notebook'
    
    if not safe_name.endswith(extension):
        safe_name += extension
    
    if output_dir:
        # Normalize and resolve paths for FUSE
        output_dir = os.path.abspath(os.path.expanduser(output_dir))
        if source_dir:
            rel_dir = os.path.dirname(os.path.relpath(input_file, source_dir))
            output_path = os.path.join(output_dir, rel_dir, safe_name) if rel_dir != '.' else os.path.join(output_dir, safe_name)
        else:
            output_path = os.path.join(output_dir, safe_name)
    else:
        output_path = os.path.join(os.path.dirname(input_file), safe_name)
    
    return os.path.normpath(output_path)

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
        
        # Generate output path
        output_path = generate_output_path(file_path, output_dir, source_dir, default_language, notebook_data['name'])
        
        # Write file with FUSE-compatible approach
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        
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
        
        return True, f"Successfully converted to {output_path}", interpreter_stats
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
