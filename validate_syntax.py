#!/usr/bin/env python3
"""Script to validate syntax of all Python files"""

import ast
import os
import sys

def check_file_syntax(filepath):
    """Check syntax of a single Python file"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        ast.parse(content)
        return True, None
        
    except SyntaxError as e:
        return False, f"Line {e.lineno}: {e.msg}"
    except Exception as e:
        return False, str(e)

def main():
    """Check all Python files for syntax errors"""
    errors = []
    checked = 0
    
    for root, dirs, files in os.walk('.'):
        dirs[:] = [d for d in dirs if not d.startswith('.') and d != '__pycache__']
        
        for file in files:
            if file.endswith('.py'):
                filepath = os.path.join(root, file)
                checked += 1
                
                is_valid, error = check_file_syntax(filepath)
                if not is_valid:
                    errors.append(f"{filepath}: {error}")
    
    print(f"Checked {checked} Python files")
    
    if errors:
        print(f"\n❌ Found {len(errors)} syntax errors:")
        for error in errors:
            print(f"  {error}")
        return 1
    else:
        print("✅ All Python files have valid syntax!")
        return 0

if __name__ == "__main__":
    sys.exit(main())
